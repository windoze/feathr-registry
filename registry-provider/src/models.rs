use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use log::debug;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    AnchorDef, AnchorFeatureDef, DerivedFeatureDef, Edge, EdgePropMutator, EdgeType, Entity,
    EntityPropMutator, EntityType, ProjectDef, SourceDef,
};

pub const PROJECT_TYPE: &str = "feathr_workspace_v1";
pub const ANCHOR_TYPE: &str = "feathr_anchor_v1";
pub const ANCHOR_FEATURE_TYPE: &str = "feathr_anchor_feature_v1";
pub const DERIVED_FEATURE_TYPE: &str = "feathr_derived_feature_v1";
pub const SOURCE_TYPE: &str = "feathr_source_v1";

fn is_default<T>(v: &T) -> bool
where
    T: Default + Eq,
{
    v == &T::default()
}

#[derive(Clone, Debug, Error)]
pub enum RegistryError {
    #[error("Entity[{0}] has incorrect type {1:?}")]
    WrongEntityType(Uuid, EntityType),

    #[error("Entity[{0}] not found")]
    EntityNotFound(String),

    #[error("Entity[{0}] doesn't exist")]
    InvalidEntity(Uuid),

    #[error("Invalid edge from [{0:?}] to [{1:?}]")]
    InvalidEdge(EntityType, EntityType),

    #[error("Cannot delete [{0}] when it still has dependents")]
    DeleteInUsed(Uuid),

    #[error("{0}")]
    FtsError(String),

    #[error("{0}")]
    ExternalStorageError(String),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EntityStatus {
    Active,
}

#[derive(Clone, Debug, Default, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityRef {
    pub guid: Uuid,
    pub type_name: String,
    pub unique_attributes: HashMap<String, String>,
}

impl Hash for EntityRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.guid.hash(state);
    }
}

impl PartialEq for EntityRef {
    fn eq(&self, other: &Self) -> bool {
        self.guid == other.guid
    }
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ValueType {
    #[serde(alias = "0")]
    UNSPECIFIED,
    #[serde(rename = "BOOLEAN", alias = "1")]
    BOOL,
    #[serde(rename = "INT", alias = "2")]
    INT32,
    #[serde(rename = "LONG", alias = "3")]
    INT64,
    #[serde(alias = "4")]
    FLOAT,
    #[serde(alias = "5")]
    DOUBLE,
    #[serde(alias = "6")]
    STRING,
    #[serde(alias = "7")]
    BYTES,
}

impl Default for ValueType {
    fn default() -> Self {
        ValueType::UNSPECIFIED
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VectorType {
    TENSOR,
}

impl Default for VectorType {
    fn default() -> Self {
        VectorType::TENSOR
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TensorCategory {
    DENSE,
    SPARSE,
}

impl Default for TensorCategory {
    fn default() -> Self {
        TensorCategory::DENSE
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureType {
    #[serde(rename = "type")]
    pub type_: VectorType,
    pub tensor_category: TensorCategory,
    pub dimension_type: Vec<ValueType>,
    pub val_type: ValueType,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TypedKey {
    pub key_column: String,
    pub key_column_type: ValueType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_column_alias: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FeatureTransformation {
    Expression {
        transform_expr: String,
    },
    WindowAgg {
        def_expr: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        agg_func: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        window: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        group_by: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        filter: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        limit: Option<u64>,
    },
    Udf {
        name: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnchorFeatureAttributes {
    pub qualified_name: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    #[serde(skip_serializing_if = "is_default", default)]
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedFeatureAttributes {
    #[serde(rename = "qualifiedName")]
    pub qualified_name: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    #[serde(skip_deserializing, skip_serializing_if = "is_default")]
    pub input_anchor_features: HashSet<EntityRef>,
    #[serde(skip_deserializing, skip_serializing_if = "is_default")]
    pub input_derived_features: HashSet<EntityRef>,
    #[serde(skip_serializing_if = "is_default", default)]
    pub tags: HashMap<String, String>,
}

impl DerivedFeatureAttributes {
    fn add_input_anchor_feature(&mut self, id: Uuid, attr: &AnchorFeatureAttributes) {
        debug!(
            "Adding anchor feature '{}' as input of derived feature '{}'",
            attr.name, self.name
        );
        self.input_anchor_features.insert(EntityRef {
            guid: id,
            type_name: ANCHOR_FEATURE_TYPE.to_string(),
            unique_attributes: [("qualifiedName".to_string(), attr.qualified_name.to_owned())]
                .into_iter()
                .collect(),
        });
    }

    fn add_input_derived_feature(&mut self, id: Uuid, attr: &DerivedFeatureAttributes) {
        debug!(
            "Adding derived feature '{}' as input of derived feature '{}'",
            attr.name, self.name
        );
        self.input_derived_features.insert(EntityRef {
            guid: id,
            type_name: ANCHOR_FEATURE_TYPE.to_string(),
            unique_attributes: [("qualifiedName".to_string(), attr.qualified_name.to_owned())]
                .into_iter()
                .collect(),
        });
    }

    fn remove_input_anchor_feature(&mut self, id: Uuid, attr: &AnchorFeatureAttributes) {
        debug!(
            "Removing anchor feature '{}' from derived feature '{}' input",
            attr.name, self.name
        );
        self.input_anchor_features.remove(&EntityRef {
            guid: id,
            ..Default::default()
        });
    }

    fn remove_input_derived_feature(&mut self, id: Uuid, attr: &DerivedFeatureAttributes) {
        debug!(
            "Removing derived feature '{}' from derived feature '{}' input",
            attr.name, self.name
        );
        self.input_derived_features.remove(&EntityRef {
            guid: id,
            ..Default::default()
        });
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnchorAttributes {
    #[serde(rename = "qualifiedName")]
    pub qualified_name: String,
    pub name: String,
    #[serde(skip_deserializing)]
    pub features: HashSet<EntityRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<EntityRef>,
    #[serde(skip_serializing_if = "is_default", default)]
    pub tags: HashMap<String, String>,
}

impl AnchorAttributes {
    fn add_anchor_feature(&mut self, id: Uuid, attr: &AnchorFeatureAttributes) {
        debug!(
            "Adding anchor feature '{}' into anchor '{}'",
            attr.name, self.name
        );
        self.features.insert(EntityRef {
            guid: id,
            type_name: ANCHOR_FEATURE_TYPE.to_string(),
            unique_attributes: [("qualifiedName".to_string(), attr.qualified_name.to_owned())]
                .into_iter()
                .collect(),
        });
    }

    fn remove_anchor_feature(&mut self, id: Uuid, attr: &AnchorFeatureAttributes) {
        debug!(
            "Removing anchor feature '{}' from anchor '{}'",
            attr.name, self.name
        );
        self.features.remove(&EntityRef {
            guid: id,
            ..Default::default()
        });
    }

    fn set_source(&mut self, id: Uuid, attr: &SourceAttributes) {
        debug!("Setting source '{}' for anchor '{}'", attr.name, self.name);
        self.source = Some(EntityRef {
            guid: id,
            type_name: SOURCE_TYPE.to_string(),
            unique_attributes: [("qualifiedName".to_string(), attr.qualified_name.to_owned())]
                .into_iter()
                .collect(),
        });
    }

    fn reset_source(&mut self, _id: Uuid, attr: &SourceAttributes) {
        debug!(
            "Resetting source '{}' for anchor '{}'",
            attr.name, self.name
        );
        self.source = None;
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectAttributes {
    #[serde(rename = "qualifiedName")]
    pub qualified_name: String,
    pub name: String,
    #[serde(default)]
    pub anchors: HashSet<EntityRef>,
    #[serde(default)]
    pub sources: HashSet<EntityRef>,
    pub anchor_features: HashSet<EntityRef>,
    pub derived_features: HashSet<EntityRef>,
    #[serde(skip_serializing_if = "is_default", default)]
    pub tags: HashMap<String, String>,
}

impl ProjectAttributes {
    fn add_anchor(&mut self, id: Uuid, attr: &AnchorAttributes) {
        debug!("Adding anchor '{}' into project '{}'", attr.name, self.name);
        self.anchors.insert(EntityRef {
            guid: id,
            type_name: ANCHOR_TYPE.to_string(),
            unique_attributes: [("qualifiedName".to_string(), attr.qualified_name.to_owned())]
                .into_iter()
                .collect(),
        });
    }

    fn add_source(&mut self, id: Uuid, attr: &SourceAttributes) {
        debug!("Adding source '{}' into project '{}'", attr.name, self.name);
        self.sources.insert(EntityRef {
            guid: id,
            type_name: SOURCE_TYPE.to_string(),
            unique_attributes: [("qualifiedName".to_string(), attr.qualified_name.to_owned())]
                .into_iter()
                .collect(),
        });
    }

    fn add_anchor_feature(&mut self, id: Uuid, attr: &AnchorFeatureAttributes) {
        debug!(
            "Adding anchor feature '{}' into project '{}'",
            attr.name, self.name
        );
        self.anchor_features.insert(EntityRef {
            guid: id,
            type_name: ANCHOR_FEATURE_TYPE.to_string(),
            unique_attributes: [("qualifiedName".to_string(), attr.qualified_name.to_owned())]
                .into_iter()
                .collect(),
        });
    }

    fn add_derived_feature(&mut self, id: Uuid, attr: &DerivedFeatureAttributes) {
        debug!(
            "Adding derived feature '{}' into project '{}'",
            attr.name, self.name
        );
        self.derived_features.insert(EntityRef {
            guid: id,
            type_name: ANCHOR_FEATURE_TYPE.to_string(),
            unique_attributes: [("qualifiedName".to_string(), attr.qualified_name.to_owned())]
                .into_iter()
                .collect(),
        });
    }

    fn remove_anchor(&mut self, id: Uuid, attr: &AnchorAttributes) {
        debug!(
            "Removing anchor '{}' from project '{}'",
            attr.name, self.name
        );
        self.anchors.remove(&EntityRef {
            guid: id,
            ..Default::default()
        });
    }

    fn remove_source(&mut self, id: Uuid, attr: &SourceAttributes) {
        debug!(
            "Removing source '{}' from project '{}'",
            attr.name, self.name
        );
        self.sources.remove(&EntityRef {
            guid: id,
            ..Default::default()
        });
    }

    fn remove_anchor_feature(&mut self, id: Uuid, attr: &AnchorFeatureAttributes) {
        debug!(
            "Removing anchor feature '{}' from project '{}'",
            attr.name, self.name
        );
        self.anchor_features.remove(&EntityRef {
            guid: id,
            ..Default::default()
        });
    }

    fn remove_derived_feature(&mut self, id: Uuid, attr: &DerivedFeatureAttributes) {
        debug!(
            "Removing anchor feature '{}' from project '{}'",
            attr.name, self.name
        );
        self.derived_features.remove(&EntityRef {
            guid: id,
            ..Default::default()
        });
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceAttributes {
    pub qualified_name: String,
    pub name: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub preprocessing: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub event_timestamp_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub timestamp_format: Option<String>,
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(skip_serializing_if = "is_default", default)]
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "typeName", content = "attributes")]
#[non_exhaustive]
pub enum Attributes {
    #[serde(rename = "feathr_anchor_feature_v1")]
    AnchorFeature(AnchorFeatureAttributes),
    #[serde(rename = "feathr_derived_feature_v1")]
    DerivedFeature(DerivedFeatureAttributes),
    #[serde(rename = "feathr_anchor_v1")]
    Anchor(AnchorAttributes),
    #[serde(rename = "feathr_source_v1")]
    Source(SourceAttributes),
    #[serde(rename = "feathr_workspace_v1")]
    Project(ProjectAttributes),
}

impl Attributes {
    pub fn clear(&mut self) {
        match self {
            Attributes::DerivedFeature(attr) => {
                attr.input_anchor_features.clear();
                attr.input_derived_features.clear();
            }
            Attributes::Anchor(attr) => {
                attr.features.clear();
            }
            Attributes::Project(attr) => {
                attr.anchors.clear();
                attr.sources.clear();
                attr.anchor_features.clear();
                attr.derived_features.clear();
            }
            _ => {}
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityProperty {
    pub guid: Uuid,
    #[serde(rename = "lastModifiedTS")]
    pub last_modified_ts: String,
    pub status: EntityStatus,
    pub display_text: String,
    pub classification_names: Vec<String>,
    pub meaning_names: Vec<String>,
    pub meanings: Vec<String>,
    pub is_incomplete: bool,
    pub labels: Vec<String>,
    #[serde(flatten)]
    pub attributes: Attributes,
}

impl EntityPropMutator for EntityProperty {
    fn new_project(id: Uuid, definition: &ProjectDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: id,
            last_modified_ts: chrono::Utc::now().timestamp().to_string(),
            status: EntityStatus::Active,
            display_text: definition.qualified_name.to_owned(),
            classification_names: Default::default(),
            meaning_names: Default::default(),
            meanings: Default::default(),
            is_incomplete: false,
            labels: Default::default(),
            attributes: Attributes::Project(ProjectAttributes {
                qualified_name: definition.qualified_name.to_owned(),
                name: definition.qualified_name.to_owned(),
                anchors: Default::default(),
                sources: Default::default(),
                anchor_features: Default::default(),
                derived_features: Default::default(),
                tags: definition.tags.to_owned(),
            }),
        })
    }
    fn new_source(id: Uuid, definition: &SourceDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: id,
            last_modified_ts: chrono::Utc::now().timestamp().to_string(),
            status: EntityStatus::Active,
            display_text: definition.name.to_owned(),
            classification_names: Default::default(),
            meaning_names: Default::default(),
            meanings: Default::default(),
            is_incomplete: false,
            labels: Default::default(),
            attributes: Attributes::Source(SourceAttributes {
                qualified_name: definition.qualified_name.to_owned(),
                name: definition.name.to_owned(),
                path: definition.path.to_owned(),
                preprocessing: definition.preprocessing.to_owned(),
                event_timestamp_column: definition.event_timestamp_column.to_owned(),
                timestamp_format: definition.timestamp_format.to_owned(),
                type_: definition.source_type.to_owned(),
                tags: definition.tags.to_owned(),
            }),
        })
    }
    fn new_anchor(id: Uuid, definition: &AnchorDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: id,
            last_modified_ts: chrono::Utc::now().timestamp().to_string(),
            status: EntityStatus::Active,
            display_text: definition.name.to_owned(),
            classification_names: Default::default(),
            meaning_names: Default::default(),
            meanings: Default::default(),
            is_incomplete: false,
            labels: Default::default(),
            attributes: Attributes::Anchor(AnchorAttributes {
                qualified_name: definition.qualified_name.to_owned(),
                name: definition.name.to_owned(),
                features: Default::default(),
                source: None,   // Will be set later by `connect`
                tags: definition.tags.to_owned(),
            }),
        })
    }
    fn new_anchor_feature(id: Uuid, definition: &AnchorFeatureDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: id,
            last_modified_ts: chrono::Utc::now().timestamp().to_string(),
            status: EntityStatus::Active,
            display_text: definition.name.to_owned(),
            classification_names: Default::default(),
            meaning_names: Default::default(),
            meanings: Default::default(),
            is_incomplete: false,
            labels: Default::default(),
            attributes: Attributes::AnchorFeature(AnchorFeatureAttributes {
                qualified_name: definition.qualified_name.to_owned(),
                name: definition.name.to_owned(),
                type_: definition.feature_type.to_owned(),
                transformation: definition.transformation.to_owned(),
                key: definition.key.to_owned(),
                tags: definition.tags.to_owned(),
            }),
        })
    }
    fn new_derived_feature(id: Uuid, definition: &DerivedFeatureDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: id,
            last_modified_ts: chrono::Utc::now().timestamp().to_string(),
            status: EntityStatus::Active,
            display_text: definition.name.to_owned(),
            classification_names: Default::default(),
            meaning_names: Default::default(),
            meanings: Default::default(),
            is_incomplete: false,
            labels: Default::default(),
            attributes: Attributes::DerivedFeature(DerivedFeatureAttributes {
                qualified_name: definition.qualified_name.to_owned(),
                name: definition.name.to_owned(),
                type_: definition.feature_type.to_owned(),
                transformation: definition.transformation.to_owned(),
                key: definition.key.to_owned(),
                input_anchor_features: Default::default(),
                input_derived_features: Default::default(),
                tags: definition.tags.to_owned(),
            }),
        })
    }

    fn connect(
        from: &mut Entity<Self>,
        from_id: Uuid,
        to: &mut Entity<Self>,
        to_id: Uuid,
        edge_type: EdgeType,
        _edge_id: Uuid,
    ) {
        if edge_type == EdgeType::Contains {
            to.containers.insert(from_id);
            match (
                &mut from.properties.attributes,
                &mut to.properties.attributes,
            ) {
                (Attributes::Anchor(from), Attributes::AnchorFeature(to)) => {
                    from.add_anchor_feature(to_id, &to);
                }
                (Attributes::Project(from), Attributes::AnchorFeature(to)) => {
                    from.add_anchor_feature(to_id, &to);
                }
                (Attributes::Project(from), Attributes::DerivedFeature(to)) => {
                    from.add_derived_feature(to_id, &to);
                }
                (Attributes::Project(from), Attributes::Anchor(to)) => {
                    from.add_anchor(to_id, &to);
                }
                (Attributes::Project(from), Attributes::Source(to)) => {
                    from.add_source(to_id, &to);
                }
                _ => {}
            }
        } else if edge_type == EdgeType::Consumes {
            match (
                &mut from.properties.attributes,
                &mut to.properties.attributes,
            ) {
                (Attributes::DerivedFeature(from), Attributes::AnchorFeature(to)) => {
                    from.add_input_anchor_feature(to_id, &to);
                }
                (Attributes::DerivedFeature(from), Attributes::DerivedFeature(to)) => {
                    from.add_input_derived_feature(to_id, &to);
                }
                (Attributes::Anchor(from), Attributes::Source(to)) => {
                    from.set_source(to_id, &to);
                }
                _ => {}
            }
        }
    }

    fn disconnect(
        from: &mut Entity<Self>,
        from_id: Uuid,
        to: &mut Entity<Self>,
        to_id: Uuid,
        edge_type: EdgeType,
        _edge_id: Uuid,
    ) {
        if edge_type == EdgeType::Contains {
            to.containers.remove(&from_id);
            match (
                &mut from.properties.attributes,
                &mut to.properties.attributes,
            ) {
                (Attributes::Anchor(from), Attributes::AnchorFeature(to)) => {
                    from.remove_anchor_feature(to_id, &to);
                }
                (Attributes::Project(from), Attributes::AnchorFeature(to)) => {
                    from.remove_anchor_feature(to_id, &to);
                }
                (Attributes::Project(from), Attributes::DerivedFeature(to)) => {
                    from.remove_derived_feature(to_id, &to);
                }
                (Attributes::Project(from), Attributes::Anchor(to)) => {
                    from.remove_anchor(to_id, &to);
                }
                (Attributes::Project(from), Attributes::Source(to)) => {
                    from.remove_source(to_id, &to);
                }
                _ => {}
            }
        } else if edge_type == EdgeType::Consumes {
            match (
                &mut from.properties.attributes,
                &mut to.properties.attributes,
            ) {
                (Attributes::DerivedFeature(from), Attributes::AnchorFeature(to)) => {
                    from.remove_input_anchor_feature(to_id, &to);
                }
                (Attributes::DerivedFeature(from), Attributes::DerivedFeature(to)) => {
                    from.remove_input_derived_feature(to_id, &to);
                }
                (Attributes::Anchor(from), Attributes::Source(to)) => {
                    from.reset_source(to_id, &to);
                }
                _ => {}
            }
        }
    }
}

impl Into<Entity<EntityProperty>> for EntityProperty {
    fn into(self) -> Entity<EntityProperty> {
        match &self.attributes {
            Attributes::AnchorFeature(AnchorFeatureAttributes {
                name,
                qualified_name,
                ..
            }) => Entity::<EntityProperty> {
                id: self.guid,
                etag: Uuid::new_v4(),
                entity_type: EntityType::AnchorFeature,
                name: name.to_owned(),
                qualified_name: qualified_name.to_owned(),
                containers: Default::default(),
                properties: self,
            },
            Attributes::DerivedFeature(DerivedFeatureAttributes {
                name,
                qualified_name,
                ..
            }) => Entity::<EntityProperty> {
                id: self.guid,
                etag: Uuid::new_v4(),
                entity_type: EntityType::DerivedFeature,
                name: name.to_owned(),
                qualified_name: qualified_name.to_owned(),
                containers: Default::default(),
                properties: self,
            },
            Attributes::Anchor(AnchorAttributes {
                name,
                qualified_name,
                ..
            }) => Entity::<EntityProperty> {
                id: self.guid,
                etag: Uuid::new_v4(),
                entity_type: EntityType::Anchor,
                name: name.to_owned(),
                qualified_name: qualified_name.to_owned(),
                containers: Default::default(),
                properties: self,
            },
            Attributes::Source(SourceAttributes {
                name,
                qualified_name,
                ..
            }) => Entity::<EntityProperty> {
                id: self.guid,
                etag: Uuid::new_v4(),
                entity_type: EntityType::Source,
                name: name.to_owned(),
                qualified_name: qualified_name.to_owned(),
                containers: Default::default(),
                properties: self,
            },
            Attributes::Project(ProjectAttributes {
                name,
                qualified_name,
                ..
            }) => Entity::<EntityProperty> {
                id: self.guid,
                etag: Uuid::new_v4(),
                entity_type: EntityType::Project,
                name: name.to_owned(),
                qualified_name: qualified_name.to_owned(),
                containers: Default::default(),
                properties: self,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeProperty {
    #[serde(rename = "relationshipId")]
    pub guid: Uuid,
    #[serde(rename = "relationshipType")]
    pub edge_type: EdgeType,
    #[serde(rename = "fromEntityId")]
    pub from: Uuid,
    #[serde(rename = "toEntityId")]
    pub to: Uuid,
}

impl EdgePropMutator for EdgeProperty {
    fn new(edge_id: Uuid, from_id: Uuid, to_id: Uuid, edge_type: EdgeType) -> Self {
        Self {
            guid: edge_id,
            edge_type,
            from: from_id,
            to: to_id,
        }
    }

    fn reflection(&self) -> Self {
        Self {
            guid: Uuid::new_v4(),
            edge_type: self.edge_type.reflection(),
            from: self.to,
            to: self.from,
        }
    }
}

impl Hash for EdgeProperty {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.edge_type.hash(state);
        self.from.hash(state);
        self.to.hash(state);
    }
}

impl Into<Edge<EdgeProperty>> for EdgeProperty {
    fn into(self) -> Edge<EdgeProperty> {
        Edge::<EdgeProperty> {
            id: self.guid,
            from: self.from,
            to: self.to,
            edge_type: self.edge_type,
            properties: self,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::models::*;

    #[test]
    fn des_trans() {
        let s = r#"{
            "filter": null,
            "agg_func": "AVG",
            "limit": null,
            "group_by": null,
            "window": "90d",
            "def_expr": "cast_float(fare_amount)"
        }"#;

        let t: FeatureTransformation = serde_json::from_str(s).unwrap();
        println!("{:#?}", t);
    }

    #[test]
    fn des_derived() {
        let s = r#"{
            "typeName": "feathr_derived_feature_v1",
            "attributes": {
                "qualifiedName": "feathr_ci_registry_12_33_182947__f_trip_time_rounded",
                "name": "f_trip_time_rounded",
                "input_derived_features": [],
                "type": "\n            type: {\n                type: TENSOR\n                tensorCategory: DENSE\n                dimensionType: []\n                valType: INT\n            }\n        ",
                "transformation": {
                    "transform_expr": "f_trip_time_duration % 10"
                },
                "input_anchor_features": [
                    {
                        "guid": "103baca1-377a-4ddf-8429-5da91026c269",
                        "typeName": "feathr_anchor_feature_v1",
                        "uniqueAttributes": {
                            "qualifiedName": "feathr_ci_registry_12_33_182947__request_features__f_trip_time_duration"
                        }
                    }
                ],
                "key": [
                    {
                        "full_name": "feathr.dummy_typedkey",
                        "key_column": "NOT_NEEDED",
                        "description": "A dummy typed key for passthrough/request feature.",
                        "key_column_alias": "NOT_NEEDED",
                        "key_column_type": "0"
                    }
                ]
            },
            "lastModifiedTS": "1",
            "guid": "c626c41c-d6c2-4b16-a267-6cdeea497c52",
            "status": "ACTIVE",
            "displayText": "f_trip_time_rounded",
            "classificationNames": [],
            "meaningNames": [],
            "meanings": [],
            "isIncomplete": false,
            "labels": []
        }"#;

        let e: EntityProperty = serde_json::from_str(s).unwrap();
        let e: Entity<EntityProperty> = e.into();
        println!("{:#?}", e);
    }

    #[test]
    fn des_entity() {
        let s = r#"{
            "typeName": "feathr_anchor_feature_v1",
            "attributes": {
                "qualifiedName": "feathr_ci_registry_12_33_182947__aggregationFeatures__f_location_avg_fare",
                "name": "f_location_avg_fare",
                "type": "\n            type: {\n                type: TENSOR\n                tensorCategory: DENSE\n                dimensionType: []\n                valType: FLOAT\n            }\n        ",
                "transformation": {
                    "filter": null,
                    "agg_func": "AVG",
                    "limit": null,
                    "group_by": null,
                    "window": "90d",
                    "def_expr": "cast_float(fare_amount)"
                },
                "key": [
                    {
                        "full_name": "nyc_taxi.location_id",
                        "key_column": "DOLocationID",
                        "description": "location id in NYC",
                        "key_column_alias": "DOLocationID",
                        "key_column_type": "2"
                    }
                ]
            },
            "lastModifiedTS": "1",
            "guid": "2a052ccd-3e31-46a7-bffb-2ab1302b1b00",
            "status": "ACTIVE",
            "displayText": "f_location_avg_fare",
            "classificationNames": [],
            "meaningNames": [],
            "meanings": [],
            "isIncomplete": false,
            "labels": []
        }"#;

        let e: EntityProperty = serde_json::from_str(s).unwrap();
        let e: Entity<EntityProperty> = e.into();
        println!("{:#?}", e);
    }
}
