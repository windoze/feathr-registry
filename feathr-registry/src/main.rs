use std::{fs::{read_dir, remove_dir_all}, path::PathBuf, process::exit};

use clap::Parser;
use common_utils::Logged;
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

    /// Base Path of the API
    #[clap(long, env = "API_BASE", default_value = "/api")]
    pub api_base: String,

    /// Init the Raft protocol so this node can be the leader of the cluster or running standalone
    #[clap(long)]
    pub init: bool,

    /// Join the cluster via seed nodes
    #[clap(long)]
    pub seeds: Vec<String>,

    /// True to join the cluster as voter, otherwise learner
    #[clap(long)]
    pub voter: bool,

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

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    common_utils::init_logger();

    // Parse the parameters passed by arguments.
    let options = Opt::parse();

    let raft_config = raft_registry::NodeConfig {
        snapshot_path: options.snapshot_path.clone(),
        instance_prefix: options.instance_prefix.clone(),
        journal_path: options.journal_path.clone(),
        snapshot_per_events: options.snapshot_per_events,
        management_code: options.management_code,
    };

    let app = if options.init {
        if !options.seeds.is_empty() {
            println!("ERROR: `seeds` must be not set when running as cluster leader");
            exit(1);
        }
        info!("Starting as cluster leader");
        // Cleanup old logs and snapshots before initializing the new cluster
        let log_path = PathBuf::from(options.journal_path)
            .join(format!("{}-1.binlog", options.instance_prefix));
        println!("Removing journal dir `{}`", log_path.to_string_lossy());
        remove_dir_all(&log_path).ok();
        read_dir(options.snapshot_path)?
            .filter(|r| {
                if let Ok(f) = r {
                    f.file_type()
                        .ok()
                        .map(|ft| ft.is_file())
                        .unwrap_or_default()
                        && f.file_name()
                            .to_str()
                            .map(|f| f.starts_with(&format!("{}+1+", options.instance_prefix)))
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
        let app = RaftRegistryApp::new(1, options.http_addr.clone(), raft_config).await;
        app.init().await?;
        match app.load_data().await {
            Ok(_) => {
                attach_storage(&mut app.store.state_machine.write().await.registry);
            },
            Err(e) => warn!("Failed to load data, error {:?}", e),
        };
        app
    } else {
        RaftRegistryApp::new(
            match options.node_id {
                Some(id) => id,
                None => {
                    println!("ERROR: Node ID must be specified.");
                    exit(1);
                }
            },
            options.http_addr.clone(),
            raft_config,
        )
        .await
    };

    let api_base = format!("/{}", options.api_base.trim_start_matches("/"));
    let http_addr = options
        .http_addr
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
    let svc_task = async {
        Server::new(TcpListener::bind(http_addr))
            .run(route)
            .await
            .log()
            .ok();
    };
    if !options.seeds.is_empty() {
        let joining_task = async {
            debug!("Joining cluster");
            app.join_cluster(&options.seeds, options.voter).await.ok();
        };
        futures::join!(svc_task, joining_task);
    } else {
        svc_task.await;
    }
    Ok(())
}
