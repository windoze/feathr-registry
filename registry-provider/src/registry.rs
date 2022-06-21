use std::{fmt::Debug, collections::HashSet};

use async_trait::async_trait;
use uuid::Uuid;

use crate::{Entity, EdgePropMutator, ToDocString, EntityPropMutator, RegistryError, EdgeType, Edge, EntityType, ProjectDef, SourceDef, AnchorDef, AnchorFeatureDef, DerivedFeatureDef, ContentEq};

#[async_trait]
pub trait RegistryProvider<EntityProp, EdgeProp> : Send + Sync
where
    // Self: Sized + Send + Sync,
    EntityProp: Clone + Debug + PartialEq + Eq + ContentEq + EntityPropMutator + ToDocString + Send + Sync,
    EdgeProp: Clone + Debug + PartialEq + Eq + EdgePropMutator + Send + Sync,
{
    /**
     * Batch load entities and edges
     */
    async fn load_data(&mut self, entities: Vec<Entity<EntityProp>>, edges: Vec<Edge<EdgeProp>>) -> Result<(), RegistryError>;

    /**
     * Get ids of all entry points
     */
    async fn get_entry_points(&self) -> Result<Vec<Entity<EntityProp>>, RegistryError>;

    /**
     * Get one entity by its id
     */
    async fn get_entity(&self, uuid: Uuid) -> Result<Entity<EntityProp>, RegistryError>;

    /**
     * Get one entity by its qualified name
     */
    async fn get_entity_by_qualified_name(
        &self,
        qualified_name: &str,
    ) -> Result<Entity<EntityProp>, RegistryError>;

    /**
     * Get multiple entities by their ids
     */
    async fn get_entities(
        &self,
        uuids: HashSet<Uuid>,
    ) -> Result<Vec<Entity<EntityProp>>, RegistryError>;

    /**
     * Get entity id by its name
     */
    async fn get_entity_id_by_qualified_name(
        &self,
        qualified_name: &str,
    ) -> Result<Uuid, RegistryError>;

    /**
     * Get all neighbors with specified connection type
     */
    async fn get_neighbors(
        &self,
        uuid: Uuid,
        edge_type: EdgeType,
    ) -> Result<Vec<Entity<EntityProp>>, RegistryError>;

    /**
     * Traversal graph from `uuid` by following edges with specific edge type
     */
    async fn bfs(
        &self,
        uuid: Uuid,
        edge_type: EdgeType,
        size_limit: usize,
    ) -> Result<(Vec<Entity<EntityProp>>, Vec<Edge<EdgeProp>>), RegistryError>;

    /**
     * Get entity ids with FTS
     */
    async fn search_entity(
        &self,
        query: &str,
        types: HashSet<EntityType>,
        scope: Option<Uuid>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Entity<EntityProp>>, RegistryError>;

    /**
     * Get all entities and connections between them under a project
     */
    async fn get_project(
        &self,
        qualified_name: &str,
    ) -> Result<(Vec<Entity<EntityProp>>, Vec<Edge<EdgeProp>>), RegistryError>;

    /**
     * Create new project
     */
    async fn new_project(&mut self, definition: &ProjectDef) -> Result<Uuid, RegistryError>;

    /**
     * Create new source under specified project
     */
    async fn new_source(
        &mut self,
        project_id: Uuid,
        definition: &SourceDef,
    ) -> Result<Uuid, RegistryError>;

    /**
     * Create new anchor under specified project
     */
    async fn new_anchor(
        &mut self,
        project_id: Uuid,
        definition: &AnchorDef,
    ) -> Result<Uuid, RegistryError>;

    /**
     * Create new anchor feature under specified anchor
     */
    async fn new_anchor_feature(
        &mut self,
        project_id: Uuid,
        anchor_id: Uuid,
        definition: &AnchorFeatureDef,
    ) -> Result<Uuid, RegistryError>;

    /**
     * Create new derived feature under specified project
     */
    async fn new_derived_feature(
        &mut self,
        project_id: Uuid,
        definition: &DerivedFeatureDef,
    ) -> Result<Uuid, RegistryError>;

    async fn delete_entity(
        &mut self,
        id: Uuid,
    ) -> Result<(), RegistryError>;

    // Provided implementations

    /**
     * Get one entity by its qualified name
     */
    async fn get_entity_by_id_or_qualified_name(
        &self,
        id_or_name: &str,
    ) -> Result<Entity<EntityProp>, RegistryError> {
        match Uuid::parse_str(&id_or_name) {
            Ok(id) => self.get_entity(id).await,
            Err(_) => self
                .get_entity_by_qualified_name(id_or_name)
                .await,
        }
    }

    /**
     * Get entity name
     */
    async fn get_entity_name(&self, uuid: Uuid) -> Result<String, RegistryError> {
        Ok(self.get_entity(uuid).await?.name)
    }

    /**
     * Get entity qualified name
     */
    async fn get_entity_qualified_name(&self, uuid: Uuid) -> Result<String, RegistryError> {
        Ok(self.get_entity(uuid).await?.qualified_name)
    }

    /**
     * Get entity type
     */
    async fn get_entity_type(&self, uuid: Uuid) -> Result<EntityType, RegistryError> {
        Ok(self.get_entity(uuid).await?.entity_type)
    }

    /**
     * Get entity id by its qualified name or id
     */
    async fn get_entity_id(&self, name_or_id: &str) -> Result<Uuid, RegistryError> {
        match Uuid::parse_str(name_or_id) {
            Ok(id) => Ok(self.get_entity(id).await?.id),
            Err(_) => self.get_entity_id_by_qualified_name(name_or_id).await,
        }
    }

    /**
     * Returns the names of all projects
     */
    async fn get_project_names(&self) -> Result<Vec<String>, RegistryError> {
        Ok(self
            .get_entry_points()
            .await?
            .into_iter()
            .filter(|e| e.entity_type == EntityType::Project)
            .map(|e| e.qualified_name)
            .collect())
    }

    /**
     * Returns all entities belong to specified project
     */
    async fn get_children(
        &self,
        id: Uuid,
        entity_types: HashSet<EntityType>,
    ) -> Result<Vec<Entity<EntityProp>>, RegistryError> {
        // Make sure the entity has correct type
        let et = self.get_entity(id).await?.entity_type;
        if et != EntityType::Project && et != EntityType::Anchor {
            return Err(RegistryError::WrongEntityType(id, et));
        }
        // Get all ids belongs to this project
        Ok(self
            .get_neighbors(id, EdgeType::Contains)
            .await?
            .into_iter()
            .filter(|e| entity_types.contains(&e.entity_type))
            .collect())
    }

    /**
     * Returns all entities that depend on this one and vice versa, directly and indirectly
     */
    async fn get_lineage(
        &self,
        id: Uuid,
        size_limit: usize,
    ) -> Result<(Vec<Entity<EntityProp>>, Vec<Edge<EdgeProp>>), RegistryError> {
        let (upstream, upstream_edges) = self.bfs(id, EdgeType::Consumes, size_limit).await?;
        let (downstream, downstream_edges) = self.bfs(id, EdgeType::Produces, size_limit).await?;
        Ok((
            upstream
                .into_iter()
                .chain(downstream.into_iter())
                .collect::<HashSet<Entity<EntityProp>>>()
                .into_iter()
                .collect(),
            upstream_edges
                .into_iter()
                .chain(downstream_edges.into_iter())
                .collect::<HashSet<Edge<EdgeProp>>>()
                .into_iter()
                .collect(),
        ))
    }
}

