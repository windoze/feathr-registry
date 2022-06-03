use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AnchorDef, AnchorFeatureDef, DerivedFeatureDef, Edge, EdgeProperty, EdgeType, Entity,
    EntityProperty, EntityType, ProjectDef, RegistryError, RegistryProvider, SourceDef,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RegistryOperator {
    GetEntryPoints,
    GetEntity {
        uuid: Uuid,
    },
    GetEntityByQualifiedName {
        qualified_name: String,
    },
    GetEntities {
        uuids: HashSet<Uuid>,
    },
    GetEntityIdByQualifiedName {
        qualified_name: String,
    },
    GetNeighbors {
        uuid: Uuid,
        edge_type: EdgeType,
    },
    Bfs {
        uuid: Uuid,
        edge_type: EdgeType,
        size_limit: usize,
    },
    SearchEntity {
        query: String,
        types: HashSet<EntityType>,
        scope: Option<Uuid>,
        limit: usize,
        offset: usize,
    },
    GetProject {
        qualified_name: String,
    },
    GetChildren {
        id: Uuid,
        entity_types: HashSet<EntityType>,
    },
    GetLineage {
        id: Uuid,
        size_limit: usize,
    },
    NewProject {
        id: Uuid,
        definition: ProjectDef,
    },
    NewSource {
        project_id: Uuid,
        id: Uuid,
        definition: SourceDef,
    },
    NewAnchor {
        project_id: Uuid,
        id: Uuid,
        definition: AnchorDef,
    },
    NewAnchorFeature {
        project_id: Uuid,
        anchor_id: Uuid,
        id: Uuid,
        definition: AnchorFeatureDef,
    },
    NewDerivedFeature {
        project_id: Uuid,
        id: Uuid,
        definition: DerivedFeatureDef,
    },
    DeleteEntity {
        id: Uuid,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RegistryResponse {
    Error(RegistryError),

    Unit,
    Uuid(Uuid),
    Entity(Entity<EntityProperty>),
    Entities(Vec<Entity<EntityProperty>>),
    EntitiesAndEdges(Vec<Entity<EntityProperty>>, Vec<Edge<EdgeProperty>>),
}

impl From<Result<(), RegistryError>> for RegistryResponse {
    fn from(v: Result<(), RegistryError>) -> Self {
        match v {
            Ok(()) => Self::Unit,
            Err(e) => Self::Error(e),
        }
    }
}

impl From<Result<Uuid, RegistryError>> for RegistryResponse {
    fn from(v: Result<Uuid, RegistryError>) -> Self {
        match v {
            Ok(v) => Self::Uuid(v),
            Err(e) => Self::Error(e),
        }
    }
}

impl From<Result<Entity<EntityProperty>, RegistryError>> for RegistryResponse {
    fn from(v: Result<Entity<EntityProperty>, RegistryError>) -> Self {
        match v {
            Ok(v) => Self::Entity(v),
            Err(e) => Self::Error(e),
        }
    }
}

impl From<Result<Vec<Entity<EntityProperty>>, RegistryError>> for RegistryResponse {
    fn from(v: Result<Vec<Entity<EntityProperty>>, RegistryError>) -> Self {
        match v {
            Ok(v) => Self::Entities(v),
            Err(e) => Self::Error(e),
        }
    }
}

impl From<Result<(Vec<Entity<EntityProperty>>, Vec<Edge<EdgeProperty>>), RegistryError>>
    for RegistryResponse
{
    fn from(
        v: Result<(Vec<Entity<EntityProperty>>, Vec<Edge<EdgeProperty>>), RegistryError>,
    ) -> Self {
        match v {
            Ok((a, b)) => Self::EntitiesAndEdges(a, b),
            Err(e) => Self::Error(e),
        }
    }
}

impl RegistryOperator {
    pub async fn apply(
        self,
        r: &mut impl RegistryProvider<EntityProperty, EdgeProperty>,
    ) -> RegistryResponse {
        match self {
            RegistryOperator::GetEntryPoints => r.get_entry_points().await.into(),
            RegistryOperator::GetEntity { uuid } => r.get_entity(uuid).await.into(),
            RegistryOperator::GetEntityByQualifiedName { qualified_name } => {
                r.get_entity_by_qualified_name(&qualified_name).await.into()
            }
            RegistryOperator::GetEntities { uuids } => r.get_entities(uuids).await.into(),
            RegistryOperator::GetEntityIdByQualifiedName { qualified_name } => {
                r.get_entity_by_qualified_name(&qualified_name).await.into()
            }
            RegistryOperator::GetNeighbors { uuid, edge_type } => {
                r.get_neighbors(uuid, edge_type).await.into()
            }
            RegistryOperator::Bfs {
                uuid,
                edge_type,
                size_limit,
            } => r.bfs(uuid, edge_type, size_limit).await.into(),
            RegistryOperator::SearchEntity {
                query,
                types,
                scope,
                limit,
                offset,
            } => r
                .search_entity(&query, types, scope, limit, offset)
                .await
                .into(),
            RegistryOperator::GetProject { qualified_name } => {
                r.get_project(&qualified_name).await.into()
            }
            RegistryOperator::GetChildren { id, entity_types } => {
                r.get_children(id, entity_types).await.into()
            }
            RegistryOperator::GetLineage { id, size_limit } => {
                r.get_lineage(id, size_limit).await.into()
            }
            RegistryOperator::NewProject { id, definition } => r.new_project(id, &definition).await.into(),
            RegistryOperator::NewSource {
                id,
                project_id,
                definition,
            } => r.new_source(project_id, id, &definition).await.into(),
            RegistryOperator::NewAnchor {
                id,
                project_id,
                definition,
            } => r.new_anchor(project_id, id, &definition).await.into(),
            RegistryOperator::NewAnchorFeature {
                id,
                project_id,
                anchor_id,
                definition,
            } => r
                .new_anchor_feature(project_id, anchor_id, id, &definition)
                .await
                .into(),
            RegistryOperator::NewDerivedFeature {
                id,
                project_id,
                definition,
            } => r.new_derived_feature(project_id, id, &definition).await.into(),
            RegistryOperator::DeleteEntity { id } => r.delete_entity(id).await.into(),
        }
    }
}
