mod database;
mod db_registry;
mod fts;
mod serdes;

#[cfg(any(mock, test))]
mod mock;

use std::collections::HashSet;
use std::fmt::Debug;

use async_trait::async_trait;
pub use database::{load_registry, attach_storage, load_content};
pub use db_registry::Registry;
use registry_provider::{
    AnchorDef, AnchorFeatureDef, DerivedFeatureDef, Edge, EdgePropMutator, EdgeType, Entity,
    EntityPropMutator, EntityType, ProjectDef, RegistryError, RegistryProvider, SourceDef,
    ToDocString,
};
use uuid::Uuid;

#[async_trait]
impl<EntityProp, EdgeProp> RegistryProvider<EntityProp, EdgeProp> for Registry<EntityProp, EdgeProp>
where
    EntityProp: Clone + Debug + PartialEq + Eq + EntityPropMutator + ToDocString + Send + Sync,
    EdgeProp: Clone + Debug + PartialEq + Eq + EdgePropMutator + Send + Sync,
{
    /**
     * Replace existing content with input snapshot
     */
    async fn load_data(
        &mut self,
        entities: Vec<Entity<EntityProp>>,
        edges: Vec<Edge<EdgeProp>>,
    ) -> Result<(), RegistryError> {
        self.batch_load(
            entities.into_iter().map(|e| e.into()),
            edges.into_iter().map(|e| e.into()),
        )
        .await
    }

    /**
     * Get ids of all entry points
     */
    async fn get_entry_points(&self) -> Result<Vec<Entity<EntityProp>>, RegistryError> {
        Ok(self
            .entry_points
            .iter()
            .filter_map(|&idx| self.graph.node_weight(idx).cloned())
            .collect())
    }

    /**
     * Get one entity by its id
     */
    async fn get_entity(&self, uuid: Uuid) -> Result<Entity<EntityProp>, RegistryError> {
        self.graph
            .node_weight(self.get_idx(uuid)?)
            .cloned()
            .ok_or_else(|| RegistryError::InvalidEntity(uuid))
    }

    /**
     * Get one entity by its qualified name
     */
    async fn get_entity_by_qualified_name(
        &self,
        qualified_name: &str,
    ) -> Result<Entity<EntityProp>, RegistryError> {
        self.get_entity_by_name(qualified_name)
            .ok_or_else(|| RegistryError::EntityNotFound(qualified_name.to_string()))
    }

    /**
     * Get multiple entities by their ids
     */
    async fn get_entities(
        &self,
        uuids: HashSet<Uuid>,
    ) -> Result<Vec<Entity<EntityProp>>, RegistryError> {
        Ok(uuids
            .into_iter()
            .filter_map(|id| {
                self.get_idx(id)
                    .ok()
                    .map(|idx| self.graph.node_weight(idx).cloned())
                    .flatten()
            })
            .collect())
    }

    /**
     * Get entity id by its name
     */
    async fn get_entity_id_by_qualified_name(
        &self,
        qualified_name: &str,
    ) -> Result<Uuid, RegistryError> {
        self.name_id_map
            .get(qualified_name)
            .ok_or_else(|| RegistryError::EntityNotFound(qualified_name.to_string()))
            .cloned()
    }

    /**
     * Get all neighbors with specified connection type
     */
    async fn get_neighbors(
        &self,
        uuid: Uuid,
        edge_type: EdgeType,
    ) -> Result<Vec<Entity<EntityProp>>, RegistryError> {
        let idx = self.get_idx(uuid)?;
        Ok(self
            .get_neighbors_idx(idx, |e| e.edge_type == edge_type)
            .into_iter()
            .filter_map(|idx| self.graph.node_weight(idx).cloned())
            .collect())
    }

    /**
     * Traversal graph from `uuid` by following edges with specific edge type
     */
    async fn bfs(
        &self,
        uuid: Uuid,
        edge_type: EdgeType,
        size_limit: usize,
    ) -> Result<(Vec<Entity<EntityProp>>, Vec<Edge<EdgeProp>>), RegistryError> {
        self.bfs_traversal(uuid, size_limit, |_| true, |e| e.edge_type == edge_type)
    }

    /**
     * Get entity ids with FTS
     */
    async fn search_entity(
        &self,
        query: &str,
        types: HashSet<EntityType>,
        container: Option<Uuid>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Entity<EntityProp>>, RegistryError> {
        Ok(self
            .fts_index
            .search(
                query,
                types.into_iter().map(|t| format!("{:?}", t)).collect(),
                container.map(|id| id.to_string()),
                limit,
                offset,
            )? // TODO:
            .into_iter()
            .filter_map(|id| self.get_entity_by_id(id))
            .take(limit)
            .collect())
    }

    /**
     * Get all entities and connections between them under a project
     */
    async fn get_project(
        &self,
        qualified_name: &str,
    ) -> Result<(Vec<Entity<EntityProp>>, Vec<Edge<EdgeProp>>), RegistryError> {
        let uuid = self.get_entity_id(qualified_name).await?;
        let (entities, edges) = self.get_project_by_id(uuid)?;
        Ok((entities.into_iter().collect(), edges.into_iter().collect()))
    }

    // Create new project
    async fn new_project(
        &mut self,
        definition: &ProjectDef,
    ) -> Result<Uuid, RegistryError> {
        // TODO: Pre-flight validation
        let prop = EntityProp::new_project(&definition)?;
        self.insert_entity(
            definition.id,
            EntityType::Project,
            &definition.qualified_name,
            &definition.qualified_name,
            prop,
        )
        .await
    }

    // Create new source under specified project
    async fn new_source(
        &mut self,
        project_id: Uuid,
        definition: &SourceDef,
    ) -> Result<Uuid, RegistryError> {
        // TODO: Pre-flight validation
        let prop = EntityProp::new_source(&definition)?;
        let source_id = self
            .insert_entity(
                definition.id,
                EntityType::Source,
                &definition.name,
                &definition.qualified_name,
                prop,
            )
            .await?;

        self.connect(
            project_id,
            source_id,
            EdgeType::Contains,
            EdgeProp::new(project_id, source_id, EdgeType::Contains),
        )?;

        Ok(source_id)
    }

    // Create new anchor under specified project
    async fn new_anchor(
        &mut self,
        project_id: Uuid,
        definition: &AnchorDef,
    ) -> Result<Uuid, RegistryError> {
        // TODO: Pre-flight validation
        let prop = EntityProp::new_anchor(&definition)?;
        let anchor_id = self
            .insert_entity(
                definition.id,
                EntityType::Anchor,
                &definition.name,
                &definition.qualified_name,
                prop,
            )
            .await?;

        self.connect(
            project_id,
            anchor_id,
            EdgeType::Contains,
            EdgeProp::new(project_id, anchor_id, EdgeType::Contains),
        )?;

        self.connect(
            anchor_id,
            definition.source_id,
            EdgeType::Consumes,
            EdgeProp::new(anchor_id, definition.source_id, EdgeType::Consumes),
        )?;

        Ok(anchor_id)
    }

    // Create new anchor feature under specified anchor
    async fn new_anchor_feature(
        &mut self,
        project_id: Uuid,
        anchor_id: Uuid,
        definition: &AnchorFeatureDef,
    ) -> Result<Uuid, RegistryError> {
        // TODO: Pre-flight validation
        let prop = EntityProp::new_anchor_feature(&definition)?;
        let feature_id = self
            .insert_entity(
                definition.id,
                EntityType::AnchorFeature,
                &definition.name,
                &definition.qualified_name,
                prop,
            )
            .await?;

        self.connect(
            project_id,
            feature_id,
            EdgeType::Contains,
            EdgeProp::new(project_id, feature_id, EdgeType::Contains),
        )?;

        self.connect(
            anchor_id,
            feature_id,
            EdgeType::Contains,
            EdgeProp::new(anchor_id, feature_id, EdgeType::Contains),
        )?;

        // Anchor feature also consumes source of the anchor
        let sources = self.get_neighbors(anchor_id, EdgeType::Consumes).await?;
        for s in sources {
            self.connect(
                feature_id,
                s.id,
                EdgeType::Consumes,
                EdgeProp::new(feature_id, s.id, EdgeType::Consumes),
            )?;
        }

        Ok(feature_id)
    }

    // Create new derived feature under specified project
    async fn new_derived_feature(
        &mut self,
        project_id: Uuid,
        definition: &DerivedFeatureDef,
    ) -> Result<Uuid, RegistryError> {
        // TODO: Pre-flight validation
        let prop = EntityProp::new_derived_feature(&definition)?;
        let feature_id = self
            .insert_entity(
                definition.id,
                EntityType::DerivedFeature,
                &definition.name,
                &definition.qualified_name,
                prop,
            )
            .await?;

        self.connect(
            project_id,
            feature_id,
            EdgeType::Contains,
            EdgeProp::new(project_id, feature_id, EdgeType::Contains),
        )?;

        for &id in definition
            .input_anchor_features
            .iter()
            .chain(definition.input_derived_features.iter())
        {
            self.connect(
                feature_id,
                id,
                EdgeType::Consumes,
                EdgeProp::new(feature_id, id, EdgeType::Consumes),
            )?;
        }

        Ok(feature_id)
    }

    async fn delete_entity(&mut self, id: Uuid) -> Result<(), RegistryError> {
        self.delete_entity_by_id(id).await
    }
}
