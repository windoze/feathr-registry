use std::{
    fs::{read_dir, remove_dir_all},
    path::PathBuf,
    pin::Pin,
    process::exit,
    vec,
};

use clap::Parser;
use common_utils::Logged;
use futures::{future::join_all, Future};
use log::{debug, info, warn};
use poem::{
    listener::TcpListener,
    middleware::{Cors, Tracing},
    EndpointExt, Route, Server,
};
use poem_openapi::OpenApiService;
use raft_registry::{management_routes, raft_routes, FeathrApi, RaftRegistryApp, RaftSequencer};
use sql_provider::attach_storage;

mod spa_endpoint;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    /// Raft Node ID
    #[clap(long, env = "NODE_ID")]
    pub node_id: Option<u64>,

    /// Server Listening Address
    #[clap(long, env = "SERVER_ADDR", default_value = "http://localhost:8000")]
    pub http_addr: String,

    /// Reported Server Listening Address, it may differ from `http_addr` when the node is behind reversed proxy or NAT
    #[clap(long, env = "EXT_SERVER_ADDR")]
    pub ext_http_addr: Option<String>,

    /// Base Path of the API
    #[clap(long, env = "API_BASE", default_value = "/api")]
    pub api_base: String,

    /// Join the cluster via seed nodes
    #[clap(long)]
    pub seeds: Vec<String>,

    /// True to join the cluster learner as, otherwise voter
    #[clap(long)]
    pub learner: bool,

    /// True to load data from the database
    #[clap(long)]
    pub load_db: bool,

    /// True to write updates to the database
    #[clap(long)]
    pub write_db: bool,

    #[clap(
        long,
        hide = true,
        env = "RAFT_SNAPSHOT_PATH",
        default_value = "/tmp/snapshot"
    )]
    pub snapshot_path: String,

    #[clap(
        long,
        hide = true,
        env = "RAFT_INSTANCE_PREFIX",
        default_value = "feathr-registry"
    )]
    pub instance_prefix: String,

    #[clap(
        long,
        hide = true,
        env = "RAFT_JOURNAL_PATH",
        default_value = "/tmp/journal"
    )]
    pub journal_path: String,

    #[clap(
        long,
        hide = true,
        env = "RAFT_SNAPSHOT_PER_EVENTS",
        default_value = "100"
    )]
    pub snapshot_per_events: u64,

    #[clap(long, hide = true, env = "RAFT_MANAGEMENT_CODE")]
    pub management_code: Option<String>,
}

/**
 * Cleanup old logs and snapshots before starting the node
 */
fn cleanup_logs(options: &Opt, node_id: u64) -> anyhow::Result<()> {
    let log_path = PathBuf::from(&options.journal_path)
        .join(format!("{}-{}.binlog", options.instance_prefix, node_id));
    println!("Removing journal dir `{}`", log_path.to_string_lossy());
    remove_dir_all(&log_path).ok();
    std::fs::create_dir_all(&options.snapshot_path).ok();
    read_dir(&options.snapshot_path)?
        .filter(|r| {
            if let Ok(f) = r {
                f.file_type()
                    .ok()
                    .map(|ft| ft.is_file())
                    .unwrap_or_default()
                    && f.file_name()
                        .to_str()
                        .map(|f| {
                            f.starts_with(&format!("{}+{}+", options.instance_prefix, node_id))
                        })
                        .unwrap_or_default()
            } else {
                false
            }
        })
        .filter_map(|f| f.ok())
        .for_each(|e| {
            println!("Removing snapshot `{}`", e.path().to_string_lossy());
            std::fs::remove_file(e.path()).ok();
        });
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    common_utils::init_logger();

    // Parse the parameters passed by arguments.
    let options = Opt::parse();

    let ext_http_addr = options
        .ext_http_addr
        .clone()
        .unwrap_or_else(|| options.http_addr.clone());

    let raft_config = raft_registry::NodeConfig {
        snapshot_path: options.snapshot_path.clone(),
        instance_prefix: options.instance_prefix.clone(),
        journal_path: options.journal_path.clone(),
        snapshot_per_events: options.snapshot_per_events,
        management_code: options.management_code.clone(),
    };

    let app = if options.seeds.is_empty() {
        info!("Starting as cluster leader");
        cleanup_logs(&options, 1).ok();
        let app = RaftRegistryApp::new(1, ext_http_addr.clone(), raft_config).await;
        app.init().await?;
        app
    } else {
        RaftRegistryApp::new(
            match options.node_id {
                Some(id) => {
                    info!("Joining cluster with node id = {}", id);
                    cleanup_logs(&options, id).ok();
                    id
                }
                None => {
                    println!("ERROR: Node ID must be specified.");
                    exit(1);
                }
            },
            ext_http_addr.clone(),
            raft_config,
        )
        .await
    };

    let api_base = format!("/{}", options.api_base.trim_start_matches("/"));
    let http_addr = ext_http_addr
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string();

    let api_service = OpenApiService::new(
        FeathrApi,
        "Feathr Registry API",
        option_env!("CARGO_PKG_VERSION").unwrap_or("<unknown>"),
    )
    .server(&format!("http://{}{}", http_addr, api_base,));
    let ui = api_service.swagger_ui();
    let spec = api_service.spec();

    let route = management_routes(raft_routes(Route::new()))
        .nest(
            api_base,
            api_service
                .with(Tracing)
                .with(RaftSequencer::new(app.store.clone()))
                .with(Cors::new()),
        )
        .nest("/docs", ui)
        .at("/spec", poem::endpoint::make_sync(move |_| spec.clone()))
        .nest(
            "/",
            spa_endpoint::SpaEndpoint::new("./static-files", "index.html"),
        )
        .data(app.clone());
    let mut tasks: Vec<Pin<Box<dyn Future<Output = ()>>>> = vec![];
    let svc_task = async {
        Server::new(TcpListener::bind(options.http_addr))
            .run(route)
            .await
            .log()
            .ok();
    };
    tasks.push(Box::pin(svc_task));
    if !options.seeds.is_empty() {
        let joining_task = async {
            debug!("Joining cluster");
            app.join_cluster(&options.seeds, !options.learner)
                .await
                .ok();
        };
        tasks.push(Box::pin(joining_task));
    }
    if options.load_db {
        let loading_task = async {
            match app.load_data().await {
                Ok(_) => {
                    if options.write_db {
                        // This is a load-write node
                        attach_storage(&mut app.store.state_machine.write().await.registry);
                    }
                }
                Err(e) => warn!("Failed to load data, error {:?}", e),
            };
        };
        tasks.push(Box::pin(loading_task));
    } else if options.write_db {
        // This is a write-only node
        attach_storage(&mut app.store.state_machine.write().await.registry);
    }
    join_all(tasks.into_iter()).await;
    Ok(())
}
