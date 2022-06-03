use std::{fmt::Debug, hash::Hash};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{EdgeType, EdgePropMutator, Edge};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EdgeProperty {
    #[serde(rename = "relationshipType")]
    pub edge_type: EdgeType,
    #[serde(rename = "fromEntityId")]
    pub from: Uuid,
    #[serde(rename = "toEntityId")]
    pub to: Uuid,
}

impl EdgePropMutator for EdgeProperty {
    fn new(from_id: Uuid, to_id: Uuid, edge_type: EdgeType) -> Self {
        Self {
            edge_type,
            from: from_id,
            to: to_id,
        }
    }

    fn reflection(&self) -> Self {
        Self {
            edge_type: self.edge_type.reflection(),
            from: self.to,
            to: self.from,
        }
    }
}

impl Into<Edge<EdgeProperty>> for EdgeProperty {
    fn into(self) -> Edge<EdgeProperty> {
        Edge::<EdgeProperty> {
            from: self.from,
            to: self.to,
            edge_type: self.edge_type,
            properties: self,
        }
    }
}
