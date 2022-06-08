use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use log::debug;
use petgraph::{
    graph::{EdgeIndex, Graph, NodeIndex},
    visit::EdgeRef,
    Directed, Direction,
};
use registry_provider::*;
use serde::Deserialize;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::fts::{FtsError, FtsIndex};

const NODE_CAPACITY: usize = 1000;

impl From<FtsError> for RegistryError {
    fn from(e: FtsError) -> Self {
        RegistryError::FtsError(e.to_string())
    }
}

/**
 * The external storage interface
 * Registry will call this interface whenever the graph state has been changed.
 */
#[async_trait]
pub trait ExternalStorage<EntityProp>: Sync + Send + Debug
where
    EntityProp: Clone + Debug + PartialEq + Eq + ToDocString,
{
    /**
     * Function will be called when a new entity is added in the graph
     * ExternalStorage may need to create the entity record in database, etc
     */
    async fn add_entity(
        &mut self,
        id: Uuid,
        entity: &Entity<EntityProp>,
    ) -> Result<(), RegistryError>;

    /**
     * Function will be called when an entity is deleted in the graph
     * ExternalStorage may need to remove the entity record from database, etc
     */
    async fn delete_entity(
        &mut self,
        id: Uuid,
        entity: &Entity<EntityProp>,
    ) -> Result<(), RegistryError>;

    /**
     * Function will be called when 2 entities are connected.
     * EntityProp has already been updated accordingly.
     * ExternalStorage may need to create the edge record in database, etc
     */
    async fn connect(
        &mut self,
        from: &Entity<EntityProp>,
        from_id: Uuid,
        to: &Entity<EntityProp>,
        to_id: Uuid,
        edge_type: EdgeType,
        edge_id: Uuid,
    ) -> Result<(), RegistryError>;

    /**
     * Function will be called when 2 entities are disconnected.
     * EntityProp has already been updated accordingly.
     * ExternalStorage may need to remove the edge record from database, etc
     */
    async fn disconnect(
        &mut self,
        from: &Entity<EntityProp>,
        from_id: Uuid,
        to: &Entity<EntityProp>,
        to_id: Uuid,
        edge_type: EdgeType,
        edge_id: Uuid,
    ) -> Result<(), RegistryError>;
}

#[derive(Debug)]
pub struct Registry<EntityProp, EdgeProp>
where
    EntityProp: Clone + Debug + PartialEq + Eq + ToDocString,
    EdgeProp: Clone + Debug + PartialEq + Eq,
{
    // The graph
    pub(crate) graph: Graph<Entity<EntityProp>, Edge<EdgeProp>, Directed>,

    // Secondary index for nodes, can be used as entry points for all entity GUIDs
    pub(crate) node_id_map: HashMap<Uuid, NodeIndex>,

    // Secondary index for nodes, can be used as entry points for all entity GUIDs
    pub(crate) name_id_map: HashMap<String, Uuid>,

    pub(crate) deleted: HashSet<Uuid>,

    // Besides arbitrary NodeIndex, entry points can be used to start a graph traversal
    // Typical entry points include Projects, Sources are possible candidates as well
    pub(crate) entry_points: Vec<NodeIndex>,

    // FTS support
    pub(crate) fts_index: FtsIndex,

    // TODO:
    pub external_storage: Vec<Arc<RwLock<dyn ExternalStorage<EntityProp>>>>,
}

impl<EntityProp, EdgeProp> Default for Registry<EntityProp, EdgeProp>
where
    EntityProp: Clone + Debug + PartialEq + Eq + ToDocString,
    EdgeProp: Clone + Debug + PartialEq + Eq,
{
    fn default() -> Self {
        Self {
            graph: Default::default(),
            node_id_map: Default::default(),
            name_id_map: Default::default(),
            deleted: Default::default(),
            entry_points: Default::default(),
            fts_index: Default::default(),
            external_storage: Default::default(),
        }
    }
}

#[allow(dead_code)]
impl<'de, EntityProp, EdgeProp> Registry<EntityProp, EdgeProp>
where
    EntityProp: Clone + Debug + PartialEq + Eq + ToDocString + Deserialize<'de>,
    EdgeProp: Clone + Debug + PartialEq + Eq + Deserialize<'de>,
{
    pub fn from_content(
        graph: Graph<Entity<EntityProp>, Edge<EdgeProp>, Directed>,
        deleted: HashSet<Uuid>,
    ) -> Self {
        let mut fts_index = FtsIndex::new();
        graph.node_weights().for_each(|w| {
            fts_index.add_doc(w).ok();
        });
        let node_id_map = graph
            .node_indices()
            .filter_map(|idx| graph.node_weight(idx).map(|w| (w.id, idx)))
            .collect();
        let name_id_map = graph
            .node_weights()
            .map(|w| (w.qualified_name.to_owned(), w.id))
            .collect();
        let entry_points = graph
            .node_indices()
            .filter(|&idx| {
                graph
                    .node_weight(idx)
                    .map(|w| (w.entity_type.is_entry_point()))
                    .unwrap_or(false)
            })
            .collect();
        Self {
            graph,
            node_id_map,
            name_id_map,
            deleted,
            entry_points,
            fts_index,
            external_storage: Default::default(),
        }
    }
}

#[allow(dead_code)]
impl<EntityProp, EdgeProp> Registry<EntityProp, EdgeProp>
where
    EntityProp: Clone + Debug + PartialEq + Eq + EntityPropMutator + ToDocString + Send + Sync,
    EdgeProp: Clone + Debug + PartialEq + Eq + EdgePropMutator + Send + Sync,
{
    pub(crate) fn new() -> Self {
        Self {
            graph: Graph::new(),
            node_id_map: Default::default(),
            name_id_map: Default::default(),
            deleted: Default::default(),
            entry_points: Default::default(),
            fts_index: FtsIndex::new(),
            external_storage: Default::default(),
        }
    }

    pub(crate) async fn load<NI, EI>(entities: NI, edges: EI) -> Result<Self, RegistryError>
    where
        NI: Iterator<Item = Entity<EntityProp>>,
        EI: Iterator<Item = Edge<EdgeProp>>,
    {
        let mut ret = Self {
            graph: Graph::with_capacity(NODE_CAPACITY * 10, NODE_CAPACITY),
            node_id_map: HashMap::with_capacity(NODE_CAPACITY),
            name_id_map: HashMap::with_capacity(NODE_CAPACITY),
            deleted: HashSet::with_capacity(NODE_CAPACITY),
            entry_points: Vec::with_capacity(NODE_CAPACITY),
            fts_index: FtsIndex::new(),
            external_storage: Default::default(),
        };

        for e in entities {
            ret.insert_entity(
                e.id,
                e.entity_type,
                e.name.clone(),
                e.qualified_name.clone(),
                e.properties.clone(),
            )
            .await?;
            ret.fts_index.add_doc(&e)?;
        }

        ret.fts_index.commit()?;

        edges.for_each(|e| {
            ret.connect(e.from, e.to, e.edge_type, e.properties).ok();
        });

        Ok(ret)
    }

    pub(crate) fn get_project_by_id(
        &self,
        uuid: Uuid,
    ) -> Result<(HashSet<Entity<EntityProp>>, HashSet<Edge<EdgeProp>>), RegistryError> {
        let root = self.get_idx(uuid)?;
        let subgraph = self.graph.filter_map(
            |idx, node| {
                self.graph
                    .edges_connecting(root, idx)
                    .find(|e| e.weight().edge_type == EdgeType::Contains)
                    .map(|_| node)
            },
            |_, e| {
                if self.has_connection_type(uuid, e.from, EdgeType::Contains)
                    || self.has_connection_type(uuid, e.to, EdgeType::Contains)
                {
                    Some(e)
                } else {
                    None
                }
            },
        );

        let entities = self
            .graph
            .node_weight(root)
            .iter()
            .chain(subgraph.node_weights())
            .map(|&w| w.to_owned())
            .collect();
        let edges = subgraph.edge_weights().map(|&w| w.to_owned()).collect();

        Ok((entities, edges))
    }

    pub(crate) fn has_connection_type(&self, from: Uuid, to: Uuid, edge_type: EdgeType) -> bool {
        if let Some(from) = self.get_idx(from).ok() {
            if let Some(to) = self.get_idx(to).ok() {
                self.graph
                    .edges_connecting(from, to)
                    .find(|e| e.weight().edge_type == edge_type)
                    .is_some()
            } else {
                false
            }
        } else {
            false
        }
    }

    pub(crate) fn get_projects(&self) -> Vec<Entity<EntityProp>> {
        self.entry_points
            .iter()
            .filter_map(|&idx| self.graph.node_weight(idx).map(|w| w.to_owned()))
            .collect()
    }

    pub(crate) fn get_features(&self) -> Vec<Entity<EntityProp>> {
        self.graph
            .node_indices()
            .filter_map(|i| {
                let n = &self.graph[i];
                if n.entity_type == EntityType::AnchorFeature
                    || n.entity_type == EntityType::DerivedFeature
                {
                    Some(n.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn get_features_by_project(&self, project: &str) -> Vec<Entity<EntityProp>> {
        self.get_entities_by_project(project, |e| {
            e.entity_type == EntityType::AnchorFeature
                || e.entity_type == EntityType::DerivedFeature
        })
    }

    pub(crate) fn get_sources_by_project(&self, project: &str) -> Vec<Entity<EntityProp>> {
        self.get_entities_by_project(project, |e| e.entity_type == EntityType::Source)
    }

    pub(crate) fn get_entities_by_project<F>(
        &self,
        project: &str,
        predicate: F,
    ) -> Vec<Entity<EntityProp>>
    where
        F: Fn(&Entity<EntityProp>) -> bool,
    {
        self.get_entry_point(|n| n.entity_type == EntityType::Project && n.name == project)
            .map(|i| {
                self.graph
                    .edges(i)
                    .filter_map(|e| {
                        if e.weight().edge_type == EdgeType::Contains {
                            self.graph.node_weight(e.target())
                        } else {
                            None
                        }
                    })
                    .filter(|&w| predicate(w))
                    .map(|w| w.to_owned())
                    .filter(|w| !self.deleted.contains(&w.id))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn get_entities<F>(&self, predicate: F) -> Vec<Entity<EntityProp>>
    where
        F: Fn(&Entity<EntityProp>) -> bool,
    {
        self.graph
            .node_weights()
            .filter(|w| predicate(&w))
            .map(|w| w.to_owned())
            .collect()
    }

    pub(crate) fn get_entity_by_id(&self, uuid: Uuid) -> Option<Entity<EntityProp>> {
        self.node_id_map
            .get(&uuid)
            .filter(|_| !self.deleted.contains(&uuid))
            .map(|&i| self.graph.node_weight(i))
            .flatten()
            .map(|w| w.to_owned())
    }

    pub(crate) fn get_entity_by_name(&self, qualified_name: &str) -> Option<Entity<EntityProp>> {
        self.name_id_map
            .get(qualified_name)
            .map(|&id| self.get_entity_by_id(id))
            .flatten()
    }

    pub(crate) fn get_feature_upstream(
        &self,
        uuid: Uuid,
        size_limit: usize,
    ) -> Result<(Vec<Entity<EntityProp>>, Vec<Edge<EdgeProp>>), RegistryError> {
        self.bfs_traversal(
            uuid,
            size_limit,
            |w| {
                !self.deleted.contains(&w.id)
                    && (w.entity_type == EntityType::AnchorFeature
                        || w.entity_type == EntityType::DerivedFeature
                        || w.entity_type == EntityType::Source)
            },
            |e| e.edge_type == EdgeType::Consumes,
        )
    }

    pub(crate) fn get_feature_downstream(
        &self,
        uuid: Uuid,
        size_limit: usize,
    ) -> Result<(Vec<Entity<EntityProp>>, Vec<Edge<EdgeProp>>), RegistryError> {
        self.bfs_traversal(
            uuid,
            size_limit,
            |w| !self.deleted.contains(&w.id) && w.entity_type == EntityType::DerivedFeature,
            |e| e.edge_type == EdgeType::Produces,
        )
    }

    pub(crate) fn bfs_traversal<FN, FE>(
        &self,
        uuid: Uuid,
        size_limit: usize,
        entity_pred: FN,
        edge_pred: FE,
    ) -> Result<(Vec<Entity<EntityProp>>, Vec<Edge<EdgeProp>>), RegistryError>
    where
        FN: Fn(&Entity<EntityProp>) -> bool,
        FE: Fn(&Edge<EdgeProp>) -> bool,
    {
        let idx = self.get_idx(uuid)?;
        let mut entities: Vec<NodeIndex> = vec![idx];
        let mut edges: Vec<EdgeIndex> = vec![];
        let mut offset: usize = 0;
        // BFS
        while entities.len() < size_limit && offset < entities.len() {
            let idx = entities[offset];
            let next_edges = self
                .graph
                .edges(idx)
                .filter(|e| edge_pred(e.weight()))
                .filter(|e| {
                    self.graph
                        .node_weight(e.target())
                        .map(|w| entity_pred(w))
                        .unwrap_or(false)
                });
            for edge in next_edges.take(size_limit - entities.len()) {
                if !edges.contains(&edge.id()) {
                    edges.push(edge.id());
                }
                if !entities.contains(&edge.target()) {
                    entities.push(edge.target());
                }
            }
            offset += 1;
        }
        Ok((
            entities
                .into_iter()
                .filter_map(|idx| self.graph.node_weight(idx).cloned())
                .collect(),
            edges
                .into_iter()
                .filter_map(|idx| self.graph.edge_weight(idx).cloned())
                .collect(),
        ))
    }

    pub async fn new_entity<T1, T2>(
        &mut self,
        entity_type: EntityType,
        name: T1,
        qualified_name: T2,
        properties: EntityProp,
    ) -> Result<Uuid, RegistryError>
    where
        T1: ToString,
        T2: ToString,
    {
        if self.name_id_map.contains_key(&qualified_name.to_string()) {
            return Err(RegistryError::EntityNameExists(qualified_name.to_string()));
        }
        let id = Uuid::new_v4();
        let idx = self
            .insert_node(
                id,
                entity_type,
                name.to_string(),
                qualified_name.to_string(),
                properties,
            )
            .await?;
        self.graph.node_weight(idx).map(|e| {
            // println!("Indexing {:#?}", e);
            self.fts_index.index(e)
        });
        Ok(id)
    }

    pub async fn insert_entity<T1, T2>(
        &mut self,
        uuid: Uuid,
        entity_type: EntityType,
        name: T1,
        qualified_name: T2,
        properties: EntityProp,
    ) -> Result<Uuid, RegistryError>
    where
        T1: ToString,
        T2: ToString,
    {
        if self.node_id_map.contains_key(&uuid) {
            return Err(RegistryError::EntityIdExists(uuid));
        }
        if self.name_id_map.contains_key(&qualified_name.to_string()) {
            return Err(RegistryError::EntityNameExists(qualified_name.to_string()));
        }
        let idx = self
            .insert_node(
                uuid,
                entity_type,
                name.to_string(),
                qualified_name.to_string(),
                properties,
            )
            .await?;
        self.graph.node_weight(idx).map(|e| self.fts_index.index(e));
        Ok(uuid)
    }

    pub async fn delete_entity_by_id(&mut self, uuid: Uuid) -> Result<(), RegistryError> {
        if self
            .graph
            .edges_directed(self.get_idx(uuid)?, Direction::Outgoing)
            .find(|e| e.weight().edge_type.is_downstream())
            .is_some()
        {
            // Check if there is anything depends on this entity
            Err(RegistryError::DeleteInUsed(uuid))
        } else {
            let idx = self.get_idx(uuid)?;
            let edges: HashSet<EdgeIndex> = self
                .get_neighbors_idx(idx, |_| true)
                .into_iter()
                .flat_map(|n| {
                    self.graph
                        .edges_connecting(idx, n)
                        .chain(self.graph.edges_connecting(n, idx))
                        .map(|e| e.id())
                })
                .collect();
            // Call entity#disconnect and update node weights in the graph accordingly
            for edge in &edges {
                let (from_idx, to_idx) = self.graph.edge_endpoints(edge.to_owned()).unwrap();
                let et = self.graph.edge_weight(edge.to_owned()).unwrap().edge_type;
                let mut from = self.graph.node_weight(from_idx).unwrap().to_owned();
                let mut to = self
                    .graph
                    .node_weight(to_idx)
                    .unwrap()
                    .to_owned()
                    .to_owned();
                let from_id = from.id;
                let to_id = to.id;
                EntityProp::disconnect(&mut from, from_id, &mut to, to_id, et);
                self.graph
                    .node_weight_mut(from_idx)
                    .map(|w| w.properties = from.properties);
                self.graph
                    .node_weight_mut(to_idx)
                    .map(|w| w.properties = to.properties);
            }
            // Call external_storage#remove_entity
            if let Some(w) = self.graph.node_weight(idx) {
                for es in &self.external_storage {
                    es.write().await.delete_entity(uuid, w).await?;
                }
            }
            self.graph.retain_edges(|_, e| !edges.contains(&e));
            // Mark deletion, we don't want to invalidate node indices as we have a reversed index
            self.deleted.insert(uuid);
            Ok(())
        }
        // TODO: How to deal with FTS?
    }

    pub fn connect(
        &mut self,
        from: Uuid,
        to: Uuid,
        edge_type: EdgeType,
        properties: EdgeProp,
    ) -> Result<(), RegistryError> {
        let from_idx = self.get_idx(from)?;
        let to_idx = self.get_idx(to)?;
        debug!(
            "Connecting '{}' and '{}', edge type: {:?}",
            self.graph
                .node_weight(from_idx)
                .map(|w| w.name.to_owned())
                .unwrap_or_default(),
            self.graph
                .node_weight(to_idx)
                .map(|w| w.name.to_owned())
                .unwrap_or_default(),
            edge_type,
        );
        match self
            .graph
            .edges_connecting(from_idx, to_idx)
            .find(|e| e.weight().edge_type == edge_type)
        {
            Some(e) => {
                debug!("Connection already exists, {:?}", e);
            }
            None => {
                self.insert_edge(edge_type, from_idx, to_idx, from, to, properties.clone());
            }
        };
        match self
            .graph
            .edges_connecting(to_idx, from_idx)
            .find(|e| e.weight().edge_type == edge_type.reflection())
        {
            Some(e) => {
                debug!("Connection already exists, {:?}", e);
            }
            None => {
                self.insert_edge(
                    edge_type.reflection(),
                    to_idx,
                    from_idx,
                    to,
                    from,
                    properties.reflection(),
                );
            }
        };
        Ok(())
    }

    pub(crate) fn get_idx(&self, uuid: Uuid) -> Result<NodeIndex, RegistryError> {
        if self.deleted.contains(&uuid) {
            return Err(RegistryError::InvalidEntity(uuid));
        }
        Ok(self
            .node_id_map
            .get(&uuid)
            .ok_or_else(|| RegistryError::InvalidEntity(uuid))?
            .to_owned())
    }

    pub(crate) fn get_neighbors_idx<F>(&self, idx: NodeIndex, predicate: F) -> Vec<NodeIndex>
    where
        F: Fn(&Edge<EdgeProp>) -> bool,
    {
        self.graph
            .edges(idx)
            .filter_map(|e| {
                if predicate(e.weight()) {
                    Some(e.target())
                } else {
                    None
                }
            })
            .collect()
    }

    fn get_entry_point<F>(&self, predicate: F) -> Option<NodeIndex>
    where
        F: Fn(&Entity<EntityProp>) -> bool,
    {
        self.entry_points
            .iter()
            .filter_map(|&i| self.graph.node_weight(i).map(|w| (i.to_owned(), w)))
            .find(|(_, n)| predicate(n))
            .map(|p| p.0)
    }

    async fn insert_node(
        &mut self,
        id: Uuid,
        entity_type: EntityType,
        name: String,
        qualified_name: String,
        properties: EntityProp,
    ) -> Result<NodeIndex, RegistryError> {
        let entity = Entity {
            id,
            entity_type,
            name,
            qualified_name: qualified_name.clone(),
            containers: Default::default(),
            properties,
        };
        for storage in &self.external_storage {
            let storage = storage.clone();
            storage.write().await.add_entity(id, &entity).await?;
        }
        let idx = self.graph.add_node(entity);
        self.node_id_map.insert(id, idx);
        self.name_id_map.insert(qualified_name, id);
        if entity_type.is_entry_point() {
            self.entry_points.push(idx);
        }
        Ok(idx)
    }

    fn insert_edge(
        &mut self,
        edge_type: EdgeType,
        from_idx: NodeIndex,
        to_idx: NodeIndex,
        from_uuid: Uuid,
        to_uuid: Uuid,
        properties: EdgeProp,
    ) -> EdgeIndex {
        let mut from = self.graph.node_weight(from_idx).unwrap().to_owned();
        let mut to = self.graph.node_weight(to_idx).unwrap().to_owned();
        EntityProp::connect(&mut from, from_uuid, &mut to, to_uuid, edge_type);
        self.graph
            .node_weight_mut(from_idx)
            .map(|w| w.properties = from.properties);
        self.graph
            .node_weight_mut(to_idx)
            .map(|w| w.properties = to.properties);

        let idx = self.graph.add_edge(
            from_idx,
            to_idx,
            Edge {
                from: from_uuid,
                to: to_uuid,
                edge_type,
                properties: properties.clone(),
            },
        );
        idx
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use async_trait::async_trait;
    use rand::Rng;
    use registry_provider::*;
    use uuid::Uuid;

    use crate::mock::load;

    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct DummyEntityProp;

    impl ToDocString for DummyEntityProp {
        fn to_doc_string(&self) -> String {
            Default::default()
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct DummyEdgeProp;

    impl EntityPropMutator for DummyEntityProp {
        fn new_project(_id: Uuid, _definition: &ProjectDef) -> Result<Self, RegistryError> {
            Ok(DummyEntityProp)
        }

        fn new_source(_id: Uuid, _definition: &SourceDef) -> Result<Self, RegistryError> {
            Ok(DummyEntityProp)
        }

        fn new_anchor(_id: Uuid, _definition: &AnchorDef) -> Result<Self, RegistryError> {
            Ok(DummyEntityProp)
        }

        fn new_anchor_feature(
            _id: Uuid,
            _definition: &AnchorFeatureDef,
        ) -> Result<Self, RegistryError> {
            Ok(DummyEntityProp)
        }

        fn new_derived_feature(
            _id: Uuid,
            _definition: &DerivedFeatureDef,
        ) -> Result<Self, RegistryError> {
            Ok(DummyEntityProp)
        }

        fn connect(
            from: &mut Entity<Self>,
            _from_id: Uuid,
            to: &mut Entity<Self>,
            _to_id: Uuid,
            edge_type: EdgeType,
        ) {
            debug!(
                "Connecting: '{}' '{:?}' '{}'",
                from.name, edge_type, to.name
            );
        }

        fn disconnect(
            from: &mut Entity<Self>,
            _from_id: Uuid,
            to: &mut Entity<Self>,
            _to_id: Uuid,
            edge_type: EdgeType,
        ) {
            debug!(
                "Disconnecting: '{}' '{:?}' '{}'",
                from.name, edge_type, to.name
            );
        }
    }

    impl EdgePropMutator for DummyEdgeProp {
        fn new(_from_id: Uuid, _to_id: Uuid, _edge_type: EdgeType) -> Self {
            DummyEdgeProp
        }

        fn reflection(&self) -> Self {
            DummyEdgeProp
        }
    }

    #[derive(Debug)]
    pub struct DummyExternalStorage;

    #[async_trait]
    impl ExternalStorage<DummyEntityProp> for DummyExternalStorage {
        async fn add_entity(
            &mut self,
            _id: Uuid,
            entity: &Entity<DummyEntityProp>,
        ) -> Result<(), RegistryError> {
            debug!("Adding entity {}", entity.name);
            Ok(())
        }

        async fn delete_entity(
            &mut self,
            _id: Uuid,
            entity: &Entity<DummyEntityProp>,
        ) -> Result<(), RegistryError> {
            debug!("Deleting entity {}", entity.name);
            Ok(())
        }

        async fn connect(
            &mut self,
            from: &Entity<DummyEntityProp>,
            _from_id: Uuid,
            to: &Entity<DummyEntityProp>,
            _to_id: Uuid,
            edge_type: EdgeType,
            _edge_id: Uuid,
        ) -> Result<(), RegistryError> {
            debug!(
                "Adding edge: '{}' '{:?}' '{}'",
                from.name, edge_type, to.name
            );
            Ok(())
        }

        async fn disconnect(
            &mut self,
            from: &Entity<DummyEntityProp>,
            _from_id: Uuid,
            to: &Entity<DummyEntityProp>,
            _to_id: Uuid,
            edge_type: EdgeType,
            _edge_id: Uuid,
        ) -> Result<(), RegistryError> {
            debug!(
                "Deleting edge: '{}' '{:?}' '{}'",
                from.name, edge_type, to.name
            );
            Ok(())
        }
    }

    async fn init() -> Registry<DummyEntityProp, DummyEdgeProp> {
        common_utils::init_logger();

        // Create new registry
        let mut r = Registry::new();
        r.external_storage
            .push(Arc::new(RwLock::new(DummyExternalStorage)));

        // Prepare some test data

        // Project 1
        let idx_prj1 = r
            .new_entity(EntityType::Project, "project1", "project1", DummyEntityProp)
            .await
            .unwrap();
        let idx_src1 = r
            .new_entity(
                EntityType::Source,
                "source1",
                "project1__source1",
                DummyEntityProp,
            )
            .await
            .unwrap();
        let idx_an1 = r
            .new_entity(
                EntityType::Anchor,
                "anchor1",
                "project1__anchor1",
                DummyEntityProp,
            )
            .await
            .unwrap();
        let idx_af1 = r
            .new_entity(
                EntityType::AnchorFeature,
                "anchor_feature1",
                "project1__anchor_feature1",
                DummyEntityProp,
            )
            .await
            .unwrap();
        let idx_af2 = r
            .new_entity(
                EntityType::AnchorFeature,
                "anchor_feature2",
                "project1__anchor_feature2",
                DummyEntityProp,
            )
            .await
            .unwrap();
        let idx_af3 = r
            .new_entity(
                EntityType::AnchorFeature,
                "anchor_feature3",
                "project1__anchor_feature3",
                DummyEntityProp,
            )
            .await
            .unwrap();
        let idx_af4 = r
            .new_entity(
                EntityType::AnchorFeature,
                "anchor_feature4",
                "project1__anchor_feature4",
                DummyEntityProp,
            )
            .await
            .unwrap();
        let idx_df1 = r
            .new_entity(
                EntityType::DerivedFeature,
                "derived_feature1",
                "project1__derived_feature1",
                DummyEntityProp,
            )
            .await
            .unwrap();
        let idx_df2 = r
            .new_entity(
                EntityType::DerivedFeature,
                "derived_feature2",
                "project1__derived_feature2",
                DummyEntityProp,
            )
            .await
            .unwrap();
        let idx_df3 = r
            .new_entity(
                EntityType::DerivedFeature,
                "derived_feature3",
                "project1__derived_feature3",
                DummyEntityProp,
            )
            .await
            .unwrap();
        r.connect(idx_prj1, idx_src1, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Project1 contains Source1
        r.connect(idx_prj1, idx_an1, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Project1 contains Anchor1
        r.connect(idx_an1, idx_src1, EdgeType::Consumes, DummyEdgeProp)
            .unwrap(); // Anchor1 consumes Source1
        r.connect(idx_prj1, idx_af1, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Project1 contains AnchorFeature1
        r.connect(idx_prj1, idx_af2, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Project1 contains AnchorFeature2
        r.connect(idx_prj1, idx_af3, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Project1 contains AnchorFeature3
        r.connect(idx_prj1, idx_af4, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Project1 contains AnchorFeature4
        r.connect(idx_an1, idx_af1, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Anchor1 contains AnchorFeature1
        r.connect(idx_an1, idx_af2, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Anchor1 contains AnchorFeature2
        r.connect(idx_an1, idx_af3, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Anchor1 contains AnchorFeature3
        r.connect(idx_an1, idx_af4, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Anchor1 contains AnchorFeature4
        r.connect(idx_src1, idx_af1, EdgeType::Produces, DummyEdgeProp)
            .unwrap(); // Source1 produces AnchorFeature1
        r.connect(idx_src1, idx_af2, EdgeType::Produces, DummyEdgeProp)
            .unwrap(); // Source1 produces AnchorFeature2
        r.connect(idx_src1, idx_af3, EdgeType::Produces, DummyEdgeProp)
            .unwrap(); // Source1 produces AnchorFeature3
        r.connect(idx_src1, idx_af4, EdgeType::Produces, DummyEdgeProp)
            .unwrap(); // Source1 produces AnchorFeature4
        r.connect(idx_prj1, idx_df1, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Project1 contains DerivedFeature1
        r.connect(idx_prj1, idx_df2, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Project1 contains DerivedFeature2
        r.connect(idx_prj1, idx_df3, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Project1 contains DerivedFeature3
        r.connect(idx_af1, idx_df1, EdgeType::Produces, DummyEdgeProp)
            .unwrap(); // AnchorFeature1 derives DerivedFeature1
        r.connect(idx_af2, idx_df2, EdgeType::Produces, DummyEdgeProp)
            .unwrap(); // AnchorFeature2 derives DerivedFeature2
        r.connect(idx_af3, idx_df2, EdgeType::Produces, DummyEdgeProp)
            .unwrap(); // AnchorFeature3 derives DerivedFeature2
        r.connect(idx_af4, idx_df3, EdgeType::Produces, DummyEdgeProp)
            .unwrap(); // AnchorFeature4 derives DerivedFeature3
        r.connect(idx_df2, idx_df3, EdgeType::Produces, DummyEdgeProp)
            .unwrap(); // DerivedFeature2 derives DerivedFeature3

        // Project 2
        let idx_prj2 = r
            .new_entity(EntityType::Project, "project2", "project2", DummyEntityProp)
            .await
            .unwrap();
        let idx_src2_1 = r
            .new_entity(
                EntityType::Source,
                "source2_1",
                "project2__source2_1",
                DummyEntityProp,
            )
            .await
            .unwrap();
        let idx_an2_1 = r
            .new_entity(
                EntityType::Anchor,
                "anchor2_1",
                "project2__anchor2_1",
                DummyEntityProp,
            )
            .await
            .unwrap();
        let idx_af2_1 = r
            .new_entity(
                EntityType::AnchorFeature,
                "anchor_feature2_1",
                "project2__anchor_feature2_1",
                DummyEntityProp,
            )
            .await
            .unwrap();
        let idx_af2_2 = r
            .new_entity(
                EntityType::AnchorFeature,
                "anchor_feature2_2",
                "project2__anchor_feature2_2",
                DummyEntityProp,
            )
            .await
            .unwrap();
        let idx_af2_3 = r
            .new_entity(
                EntityType::AnchorFeature,
                "anchor_feature2_3",
                "project2__anchor_feature2_3",
                DummyEntityProp,
            )
            .await
            .unwrap();
        r.connect(idx_prj2, idx_src2_1, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Project2 contains Source2_1
        r.connect(idx_prj2, idx_an2_1, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Project2 contains Anchor2_1
        r.connect(idx_an2_1, idx_src2_1, EdgeType::Consumes, DummyEdgeProp)
            .unwrap(); // Anchor2_1 consumes Source2_1
        r.connect(idx_prj2, idx_af2_1, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Project2 contains AnchorFeature2_1
        r.connect(idx_prj2, idx_af2_2, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Project2 contains AnchorFeature2_2
        r.connect(idx_prj2, idx_af2_3, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Project2 contains AnchorFeature2_3
        r.connect(idx_an2_1, idx_af2_1, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Anchor2_1 contains AnchorFeature2_1
        r.connect(idx_an2_1, idx_af2_2, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Anchor2_1 contains AnchorFeature2_2
        r.connect(idx_an2_1, idx_af2_3, EdgeType::Contains, DummyEdgeProp)
            .unwrap(); // Anchor2_1 contains AnchorFeature2_3
        r.connect(idx_src2_1, idx_af2_1, EdgeType::Produces, DummyEdgeProp)
            .unwrap(); // Source2_1 produces AnchorFeature2_1
        r.connect(idx_src2_1, idx_af2_2, EdgeType::Produces, DummyEdgeProp)
            .unwrap(); // Source2_1 produces AnchorFeature2_2
        r.connect(idx_src2_1, idx_af2_3, EdgeType::Produces, DummyEdgeProp)
            .unwrap(); // Source2_1 produces AnchorFeature2_3

        r
    }

    #[tokio::test]
    async fn test() {
        let r = init().await;
        let mut names: Vec<String> = r
            .get_features_by_project("project1")
            .into_iter()
            .map(|n| n.name.clone())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "anchor_feature1",
                "anchor_feature2",
                "anchor_feature3",
                "anchor_feature4",
                "derived_feature1",
                "derived_feature2",
                "derived_feature3",
            ]
        );
        let mut names: Vec<String> = r
            .get_features_by_project("project2")
            .into_iter()
            .map(|n| n.name.clone())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "anchor_feature2_1",
                "anchor_feature2_2",
                "anchor_feature2_3",
            ]
        );
    }

    #[tokio::test]
    async fn linage() {
        let r = init().await;
        let df2 = r
            .get_features_by_project("project1")
            .into_iter()
            .find(|e| e.name == "derived_feature2")
            .map(|e| e.id)
            .unwrap();
        let (entities, edges) = r.get_feature_upstream(df2, 50).unwrap();
        let mut upstream_names: Vec<String> = entities
            .into_iter()
            .map(|w| format!("{}", w.name))
            .collect();
        upstream_names.sort();
        assert_eq!(
            upstream_names,
            [
                "anchor_feature2",
                "anchor_feature3",
                "derived_feature2",
                "source1"
            ]
        );
        let mut upstream_edges: Vec<String> = edges
            .into_iter()
            .map(|e| {
                format!(
                    "{} {:?} {}",
                    r.get_entity_by_id(e.from).unwrap().name,
                    e.edge_type,
                    r.get_entity_by_id(e.to).unwrap().name
                )
            })
            .collect();
        upstream_edges.sort();
        assert_eq!(
            upstream_edges,
            [
                "anchor_feature2 Consumes source1",
                "anchor_feature3 Consumes source1",
                "derived_feature2 Consumes anchor_feature2",
                "derived_feature2 Consumes anchor_feature3"
            ]
        );
    }

    #[tokio::test]
    #[ignore = "too slow"]
    async fn many_nodes() {
        let start = Instant::now();
        // 100 Anchors
        const ANCHORS: usize = 100;
        // 1000 Anchor features per Anchor
        const ANCHOR_FEATURES: usize = 1000;
        // 10000 Derived features
        const DERIVES: usize = 10000;
        let mut r: Registry<DummyEntityProp, DummyEdgeProp> = Registry::new();
        // FTS is very slow to insert doc one by one, so we disable it for now
        r.fts_index.enable(false);
        let prj1 = r
            .new_entity(EntityType::Project, "project1", "project1", DummyEntityProp)
            .await
            .unwrap();
        let mut features: Vec<Uuid> = Vec::with_capacity(ANCHORS * ANCHOR_FEATURES + DERIVES);
        // create 100 anchor groups
        for i in 0..ANCHORS {
            let start = Instant::now();
            println!("Anchor {}", i);
            let an = r
                .new_entity(
                    EntityType::Anchor,
                    "anchor1",
                    format!("project1__anchor{}", i),
                    DummyEntityProp,
                )
                .await
                .unwrap();
            r.connect(prj1, an, EdgeType::Contains, DummyEdgeProp)
                .unwrap();
            // create 1000 anchor features in each group
            for j in 0..ANCHOR_FEATURES {
                let f = r
                    .new_entity(
                        EntityType::AnchorFeature,
                        format!("anchor{}_feature{}", i, j),
                        format!("project1__anchor{}__anchorfeature{}", i, j),
                        DummyEntityProp,
                    )
                    .await
                    .unwrap();
                features.push(f);
                r.connect(f, prj1, EdgeType::BelongsTo, DummyEdgeProp)
                    .unwrap();
                r.connect(f, an, EdgeType::BelongsTo, DummyEdgeProp)
                    .unwrap();
            }
            let end = Instant::now();
            println!("Took {} ms", (end - start).as_millis());
        }
        let mut rng = rand::thread_rng();
        // Create 10000 derived features
        for i in 0..DERIVES {
            if (i + 1) % 1000 == 0 {
                println!("{} derived features", i);
            }

            let f = r
                .new_entity(
                    EntityType::DerivedFeature,
                    "anchor1",
                    format!("project1__derivedfeature{}", i),
                    DummyEntityProp,
                )
                .await
                .unwrap();
            // Randomly pick some input features
            let count: usize = rng.gen_range(2..10);
            for _ in 0..count {
                let id = features[rng.gen_range(0..features.len())];
                r.connect(f, id, EdgeType::Consumes, DummyEdgeProp).unwrap();
            }
            features.push(f);
            r.connect(f, prj1, EdgeType::BelongsTo, DummyEdgeProp)
                .unwrap();
        }
        let end = Instant::now();
        let time = end - start;
        println!("{} seconds", time.as_secs());
    }

    #[tokio::test]
    async fn deletion() {
        let mut r: Registry<DummyEntityProp, DummyEdgeProp> = Registry::new();
        r.external_storage
            .push(Arc::new(RwLock::new(DummyExternalStorage)));
        let prj1 = r
            .new_entity(EntityType::Project, "project1", "project1", DummyEntityProp)
            .await
            .unwrap();
        let src1 = r
            .new_entity(
                EntityType::Source,
                "source1",
                "project1__source1",
                DummyEntityProp,
            )
            .await
            .unwrap();
        let an1 = r
            .new_entity(
                EntityType::Anchor,
                "anchor1",
                "project1__anchor1",
                DummyEntityProp,
            )
            .await
            .unwrap();

        r.connect(prj1, src1, EdgeType::Contains, DummyEdgeProp)
            .unwrap();
        r.connect(prj1, an1, EdgeType::Contains, DummyEdgeProp)
            .unwrap();
        r.connect(src1, an1, EdgeType::Produces, DummyEdgeProp)
            .unwrap();

        // Now graph should have 3 nodes and 3 edges

        // This should fail as source1 is used by anchor1
        assert!(r.delete_entity_by_id(src1).await.is_err());

        // This works
        r.delete_entity_by_id(an1).await.unwrap();

        // Now only edges between project1 and source1 remain
        assert_eq!(r.graph.edge_count(), 2);
    }

    #[tokio::test]
    async fn test_load() {
        let r = load().await;

        let uid = r
            .get_entity_by_name("feathr_ci_registry_12_33_182947__f_trip_time_distance")
            .unwrap()
            .id;
        assert_eq!(
            uid,
            Uuid::parse_str("226b42ee-0c34-4329-b935-744aecc63fb4").unwrap()
        );

        let (f, e) = r.get_feature_upstream(uid, 100).unwrap();
        println!("{:#?}\n{:#?}", f, e);
    }

    #[tokio::test]
    async fn test_dump() {
        let r = load().await;
        r.graph.node_weights().for_each(|w| {
            println!(
                "insert into entities (entity_id, entity_content) values ('{}', '{}');",
                w.id,
                serde_json::to_string(&w.properties).unwrap()
            );
        });
        println!("-----------------------------");
        r.graph.edge_weights().for_each(|w| {
            println!("insert into edges (edge_id, from_id, to_id, edge_type) values ('{}', '{}', '{}', '{:?}');", Uuid::new_v4(), w.from, w.to, w.edge_type);
        });
    }
}
