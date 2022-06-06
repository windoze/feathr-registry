use std::collections::{BTreeSet, BTreeMap};

use openraft::{Node, RaftMetrics, error::Infallible};
use poem::{handler, web::{Data, Json}, IntoResponse, Route, post, get};

use crate::{RaftRegistryApp, RegistryNodeId, RegistryTypeConfig};

#[handler]
pub async fn add_learner(
    app: Data<&RaftRegistryApp>,
    req: Json<(RegistryNodeId, String)>,
) -> poem::Result<impl IntoResponse> {
    let node_id = req.0 .0;
    let node = Node {
        addr: req.0 .1.clone(),
        ..Default::default()
    };
    let res = app.raft.add_learner(node_id, Some(node), true).await;
    Ok(Json(res))
}

/// Changes specified learners to members, or remove members.
#[handler]
pub async fn change_membership(
    app: Data<&RaftRegistryApp>,
    req: Json<BTreeSet<RegistryNodeId>>,
) -> poem::Result<impl IntoResponse> {
    let res = app.raft.change_membership(req.0, true, false).await;
    Ok(Json(res))
}

/// Initialize a single-node cluster.
#[handler]
pub async fn init(app: Data<&RaftRegistryApp>) -> poem::Result<impl IntoResponse> {
    let mut nodes = BTreeMap::new();
    nodes.insert(
        app.id,
        Node {
            addr: app.addr.clone(),
            data: Default::default(),
        },
    );
    let res = app.raft.initialize(nodes).await;
    Ok(Json(res))
}

/// Get the latest metrics of the cluster
#[handler]
pub async fn metrics(app: Data<&RaftRegistryApp>) -> poem::Result<impl IntoResponse> {
    let metrics = app.raft.metrics().borrow().clone();

    let res: Result<RaftMetrics<RegistryTypeConfig>, Infallible> = Ok(metrics);
    Ok(Json(res))
}

pub fn management_routes(route: Route) -> Route {
    route
        .at("/add-learner", post(add_learner))
        .at("/change-membership", post(change_membership))
        .at("/init", post(init))
        .at("/metrics", get(metrics))
}
