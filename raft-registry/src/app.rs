use std::sync::Arc;

use log::debug;
use log::trace;
use openraft::Config;
use openraft::EntryPayload;
use openraft::error::CheckIsLeaderError;
use openraft::raft::ClientWriteRequest;
use registry_api::ApiError;
use registry_api::FeathrApiProvider;
use registry_api::FeathrApiRequest;
use registry_api::FeathrApiResponse;

use crate::ManagementCode;
use crate::RegistryClient;
use crate::RegistryNodeId;
use crate::RegistryRaft;
use crate::RegistryStore;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
#[derive(Clone)]
pub struct RaftRegistryApp {
    pub id: RegistryNodeId,
    pub addr: String,
    pub raft: RegistryRaft,
    pub store: Arc<RegistryStore>,
    pub config: Arc<Config>,
    pub forwarder: RegistryClient,
}

impl RaftRegistryApp {
    pub async fn check_code(&self, code: Option<ManagementCode>) -> poem::Result<()> {
        debug!("Checking code {:?}", code);
        match self.store.get_management_code() {
            Some(c) => {
                match code.map(|c| c.code().to_string()) {
                    Some(code) => {
                        if c == code {
                            return Ok(());
                        } else {
                            return Err(ApiError::Forbidden("forbidden".to_string()))?;
                        }
                    }
                    None => return Err(ApiError::Forbidden("forbidden".to_string()))?,
                }
            }
            None => return Ok(()),
        }
    }

    pub async fn request(
        &self,
        opt_seq: Option<u64>,
        req: FeathrApiRequest,
    ) -> FeathrApiResponse {
        let mut is_leader = true;
        let should_forward = match self.raft.is_leader().await {
            Ok(_) => {
                // This instance is the leader, do not forward
                trace!("This node is the leader");
                false
            }
            Err(CheckIsLeaderError::ForwardToLeader(node_id)) => {
                debug!("Should forward the request to node {}", node_id);
                is_leader = false;
                match opt_seq {
                    Some(seq) => match self.store.state_machine.read().await.last_applied_log {
                        Some(l) => {
                            // Check is local log index is newer than required seq, forward if local is out dated
                            trace!("Local log index is {}, required seq is {}", l.index, seq);
                            l.index < seq
                        }
                        None => {
                            // There is no local log index, so we have to forward
                            trace!("No last applied log");
                            true
                        }
                    },
                    // opt_seq is not set, forward to the leader for consistent read
                    None => true,
                }
            }
            Err(e) => {
                trace!("Check leader failed, error is {:?}", e);
                false
                // return FeathrApiResponse::Error(ApiError::InternalError("Raft cluster error".to_string()));
            }
        };
        if should_forward {
            debug!("The request is being forwarded to the leader");
            match self.forwarder.consistent_request(&req).await {
                Ok(v) => v,
                Err(e) => FeathrApiResponse::Error(ApiError::InternalError(format!("{:?}", e))),
            }
        } else {
            debug!("The request is being handled locally");
            // Only writing requests need to go to raft state machine
            if req.is_writing_request() {
                if is_leader {
                    let request = ClientWriteRequest::new(EntryPayload::Normal(req));
                    self.raft
                        .client_write(request)
                        .await
                        .map(|r| r.data)
                        .unwrap_or_else(|e| FeathrApiResponse::Error( ApiError::InternalError(format!("{:?}", e))))
                } else {
                    FeathrApiResponse::Error(ApiError::BadRequest(
                        "Updating requests must be submitted to the Raft leader".to_string(),
                    ))
                }
            } else {
                self.store
                    .state_machine
                    .write()
                    .await
                    .registry
                    .request(req)
                    .await
            }
        }
    }
}
