use std::process::exit;

use clap::Parser;
use common_utils::Logged;
use log::info;
use poem::{
    listener::TcpListener,
    middleware::{Cors, Tracing},
    EndpointExt, Route, Server,
};
use poem_openapi::OpenApiService;
use raft_registry::{management_routes, raft_routes, FeathrApi, RaftRegistryApp, RaftSequencer};

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
    pub standalone: bool,

    #[clap(long, hide = true, env = "RAFT_SNAPSHOT_PATH", default_value = "/tmp/snapshot")]
    pub snapshot_path: String,
    
    #[clap(long, hide = true, env = "RAFT_INSTANCE_PREFIX", default_value = "feathr-registry")]
    pub instance_prefix: String,

    #[clap(long, hide = true, env = "RAFT_JOURNAL_PATH", default_value = "/tmp/journal")]
    pub journal_path: String,

    #[clap(long, hide = true, env = "RAFT_SNAPSHOT_PER_EVENTS", default_value = "500")]
    pub snapshot_per_events: u32,

    #[clap(long, hide = true, env = "RAFT_MANAGEMENT_CODE")]
    pub management_code: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    common_utils::init_logger();

    // Parse the parameters passed by arguments.
    let options = Opt::parse();

    let raft_config = raft_registry::Config {
        snapshot_path: options.snapshot_path,
        instance_prefix: options.instance_prefix,
        journal_path: options.journal_path,
        snapshot_per_events: options.snapshot_per_events,
        management_code: options.management_code,
    };

    let app = if options.standalone {
        info!("Starting in standalone mode");
        let app = RaftRegistryApp::new(1, options.http_addr.clone(), raft_config).await;
        app.init().await?;
        app
    } else {
        RaftRegistryApp::new(
            match options.node_id {
                Some(id) => id,
                None => {
                    println!("ERROR: Node ID must be specified.");
                    exit(1);
                },
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
        .trim_start_matches("https://");

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
        .data(app);
    Server::new(TcpListener::bind(http_addr))
        .run(route)
        .await
        .log()?;
    Ok(())
}
