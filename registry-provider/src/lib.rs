mod fts;
mod models;

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use fts::*;
pub use models::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectDef {
    pub qualified_name: String,
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceDef {
    pub name: String,
    pub qualified_name: String,
    #[serde(rename = "type")]
    pub source_type: String,
    pub path: String,
    pub event_timestamp_column: Option<String>,
    pub timestamp_format: Option<String>,
    pub preprocessing: Option<String>,
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnchorDef {
    pub name: String,
    pub qualified_name: String,
    pub source_id: Uuid,
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnchorFeatureDef {
    pub name: String,
    pub qualified_name: String,
    pub feature_type: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedFeatureDef {
    pub name: String,
    pub qualified_name: String,
    pub feature_type: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    pub input_anchor_features: HashSet<Uuid>,
    pub input_derived_features: HashSet<Uuid>,
    pub tags: HashMap<String, String>,
}

#[async_trait]
pub trait RegistryProvider<EntityProp, EdgeProp>
where
    Self: Sized + Send + Sync,
    EntityProp: Clone + Debug + PartialEq + Eq + EntityPropMutator + ToDocString + Send + Sync,
    EdgeProp: Clone + Debug + PartialEq + Eq + EdgePropMutator + Send + Sync,
{
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
    ) -> Option<Entity<EntityProp>>;

    /**
     * Get multiple entities by their ids
     */
    async fn get_entities(
        &self,
        uuids: HashSet<Uuid>,
    ) -> Result<HashMap<Uuid, Entity<EntityProp>>, RegistryError>;

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

    // Provided implementations

    /**
     * Get entity id by its qualified name or id
     */
    async fn get_entity_id(&self, name_or_id: &str) -> Result<Uuid, RegistryError> {
        match Uuid::parse_str(name_or_id) {
            Ok(id) => {
                Ok(self.get_entity(id).await?.id)
            }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EntityType {
    Unknown,

    Project,
    Source,
    Anchor,
    AnchorFeature,
    DerivedFeature,
}

impl EntityType {
    pub fn is_entry_point(self) -> bool {
        match self {
            EntityType::Project => true,
            _ => false,
        }
    }
}

impl Default for EntityType {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EdgeType {
    // Feature/Source/AnchorGroup belongs to project
    BelongsTo,
    // Project Contains Feature/Source/AnchorGroup, AnchorGroup contains AnchorFeatures
    Contains,

    // AnchorGroup uses Source, DerivedFeature used Anchor/DerivedFeatures
    Consumes,
    // Source used by AnchorGroup, Anchor/DerivedFeatures derives DerivedFeature
    Produces,
}

impl Default for EdgeType {
    fn default() -> Self {
        EdgeType::BelongsTo // Whatever
    }
}

impl EdgeType {
    pub fn reflection(self) -> Self {
        match self {
            EdgeType::BelongsTo => EdgeType::Contains,
            EdgeType::Contains => EdgeType::BelongsTo,
            EdgeType::Consumes => EdgeType::Produces,
            EdgeType::Produces => EdgeType::Consumes,
        }
    }

    pub fn is_downstream(self) -> bool {
        matches!(self, EdgeType::Contains | EdgeType::Produces)
    }

    pub fn is_upstream(self) -> bool {
        matches!(self, EdgeType::BelongsTo | EdgeType::Consumes)
    }

    pub fn validate(&self, from: EntityType, to: EntityType) -> bool {
        match (from, to, self) {
            (EntityType::Project, EntityType::Source, EdgeType::Contains)
            | (EntityType::Project, EntityType::Anchor, EdgeType::Contains)
            | (EntityType::Project, EntityType::AnchorFeature, EdgeType::Contains)
            | (EntityType::Project, EntityType::DerivedFeature, EdgeType::Contains)
            | (EntityType::Source, EntityType::Project, EdgeType::BelongsTo)
            | (EntityType::Source, EntityType::Anchor, EdgeType::Produces)
            | (EntityType::Source, EntityType::AnchorFeature, EdgeType::Produces)
            | (EntityType::Anchor, EntityType::Project, EdgeType::BelongsTo)
            | (EntityType::Anchor, EntityType::Source, EdgeType::Consumes)
            | (EntityType::Anchor, EntityType::AnchorFeature, EdgeType::Contains)
            | (EntityType::AnchorFeature, EntityType::Project, EdgeType::BelongsTo)
            | (EntityType::AnchorFeature, EntityType::Source, EdgeType::Consumes)
            | (EntityType::AnchorFeature, EntityType::Anchor, EdgeType::BelongsTo)
            | (EntityType::AnchorFeature, EntityType::DerivedFeature, EdgeType::Produces)
            | (EntityType::DerivedFeature, EntityType::Project, EdgeType::BelongsTo)
            | (EntityType::DerivedFeature, EntityType::AnchorFeature, EdgeType::Consumes)
            | (EntityType::DerivedFeature, EntityType::DerivedFeature, EdgeType::Produces) => true,

            _ => return false,
        }
    }
}

#[derive(Clone, Debug, Eq)]
pub struct Entity<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    pub id: Uuid,
    pub etag: Uuid,
    pub entity_type: EntityType,
    pub name: String,
    pub qualified_name: String,
    pub containers: HashSet<Uuid>,
    pub properties: Prop,
}

impl<Prop> PartialEq for Entity<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<Prop> Hash for Entity<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Clone, Debug, Eq)]
pub struct Edge<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    pub id: Uuid,
    pub from: Uuid,
    pub to: Uuid,
    pub edge_type: EdgeType,
    pub properties: Prop,
}

impl<Prop> PartialEq for Edge<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<Prop> Hash for Edge<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

pub trait EntityPropMutator
where
    Self: Clone + Debug + PartialEq + Eq + crate::fts::ToDocString,
{
    fn new_project(id: Uuid, definition: &ProjectDef) -> Result<Self, RegistryError>;
    fn new_source(id: Uuid, definition: &SourceDef) -> Result<Self, RegistryError>;
    fn new_anchor(id: Uuid, definition: &AnchorDef) -> Result<Self, RegistryError>;
    fn new_anchor_feature(id: Uuid, definition: &AnchorFeatureDef) -> Result<Self, RegistryError>;
    fn new_derived_feature(id: Uuid, definition: &DerivedFeatureDef)
        -> Result<Self, RegistryError>;

    /**
     * Function will be called when 2 entities are connected.
     * EntityProp may need to update internal state accordingly.
     */
    fn connect(
        from: &mut Entity<Self>,
        from_id: Uuid,
        to: &mut Entity<Self>,
        to_id: Uuid,
        edge_type: EdgeType,
        edge_id: Uuid,
    );

    /**
     * Function will be called when 2 entities are disconnected.
     * EntityProp may need to update internal state accordingly.
     */
    fn disconnect(
        from: &mut Entity<Self>,
        from_id: Uuid,
        to: &mut Entity<Self>,
        to_id: Uuid,
        edge_type: EdgeType,
        edge_id: Uuid,
    );
}

pub trait EdgePropMutator
where
    Self: Clone + Debug + PartialEq + Eq,
{
    fn new(edge_id: Uuid, from_id: Uuid, to_id: Uuid, edge_type: EdgeType) -> Self;

    /**
     * Get the refection of this edge, e.g. (A contains B) -> (B belongsTo A)
     */
    fn reflection(&self) -> Self;
}
