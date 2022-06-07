use std::sync::Arc;

use clap::Parser;
use common_utils::Logged;
use openraft::{Config, Raft, SnapshotPolicy};
use poem::{
    listener::TcpListener,
    middleware::{Cors, Tracing},
    EndpointExt, Route, Server,
};
use poem_openapi::OpenApiService;
use raft_registry::{
    management_routes, raft_routes, FeathrApi, RaftRegistryApp, RaftSequencer, RegistryClient,
    RegistryNetwork, RegistryNodeId, RegistryStore, Restore,
};

pub async fn init_app(node_id: RegistryNodeId, addr: String) -> RaftRegistryApp {
    // Create a configuration for the raft instance.

    let mut config = Config::default().validate().unwrap();
    config.snapshot_policy = SnapshotPolicy::LogsSinceLast(500);
    config.max_applied_log_to_keep = 20000;
    config.install_snapshot_timeout = 400;

    let config = Arc::new(config);

    // Create a instance of where the Raft data will be stored.
    let es = RegistryStore::open_create(node_id);

    // es.load_latest_snapshot().await.unwrap();

    let mut store = Arc::new(es);

    store.restore().await;

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = RegistryNetwork::new(store.get_management_code());

    // Create a local raft instance.
    let raft = Raft::new(node_id, config.clone(), network, store.clone());

    let forwarder = RegistryClient::new(node_id, addr.clone(), store.get_management_code());

    // Create an application that will store all the instances created above, this will
    // be later used on the web services.
    RaftRegistryApp {
        id: node_id,
        addr,
        raft,
        store,
        config,
        forwarder,
    }
}

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub id: u64,

    #[clap(long)]
    pub http_addr: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    common_utils::init_logger();

    // Parse the parameters passed by arguments.
    let options = Opt::parse();

    let app = init_app(options.id, options.http_addr.clone()).await;

    let api_service = OpenApiService::new(FeathrApi, "TestApi", "1.0.0").server(&format!(
        "http://{}/api",
        options.http_addr.trim_start_matches("http://")
    ));
    let ui = api_service.swagger_ui();
    let spec = api_service.spec();
    let route = management_routes(raft_routes(Route::new()))
        .nest("/api", api_service.with(Tracing))
        .nest("/docs", ui)
        .at("/spec", poem::endpoint::make_sync(move |_| spec.clone()))
        .with(RaftSequencer::new(app.store.clone()))
        .with(Cors::new())
        .data(app);
    Server::new(TcpListener::bind(options.http_addr))
        .run(route)
        .await
        .log()?;
    Ok(())
}
