use std::collections::HashMap;

use common_utils::FlippedOptionResult;
use poem_openapi::{Enum, Object, Union};
use registry_provider::{EdgeProperty, EntityProperty};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ApiError;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Enum)]
pub enum EntityType {
    #[oai(rename="unknown")]
    Unknown,

    #[oai(rename="feathr_workspace_v1")]
    Project,
    #[oai(rename="feathr_source_v1")]
    Source,
    #[oai(rename="feathr_anchor_v1")]
    Anchor,
    #[oai(rename="feathr_anchor_feature_v1")]
    AnchorFeature,
    #[oai(rename="feathr_derived_feature_v1")]
    DerivedFeature,
}

impl From<registry_provider::EntityType> for EntityType {
    fn from(v: registry_provider::EntityType) -> Self {
        match v {
            registry_provider::EntityType::Unknown => EntityType::Unknown,
            registry_provider::EntityType::Project => EntityType::Project,
            registry_provider::EntityType::Source => EntityType::Source,
            registry_provider::EntityType::Anchor => EntityType::Anchor,
            registry_provider::EntityType::AnchorFeature => EntityType::AnchorFeature,
            registry_provider::EntityType::DerivedFeature => EntityType::DerivedFeature,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
pub struct Entities {
    pub entities: Vec<Entity>,
}

impl FromIterator<registry_provider::Entity<EntityProperty>> for Entities {
    fn from_iter<T: IntoIterator<Item = registry_provider::Entity<EntityProperty>>>(
        iter: T,
    ) -> Self {
        Self {
            entities: iter.into_iter().map(|e| e.into()).collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct EntityRef {
    guid: String,
    type_name: String,
    unique_attributes: HashMap<String, String>,
}

impl From<registry_provider::EntityRef> for EntityRef {
    fn from(v: registry_provider::EntityRef) -> Self {
        Self {
            guid: v.guid.to_string(),
            type_name: v.type_name,
            unique_attributes: v.unique_attributes,
        }
    }
}

impl TryInto<registry_provider::EntityRef> for EntityRef {
    type Error = ApiError;

    fn try_into(self) -> Result<registry_provider::EntityRef, Self::Error> {
        Ok(registry_provider::EntityRef {
            guid: parse_uuid(&self.guid)?,
            type_name: self.type_name,
            unique_attributes: self.unique_attributes,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct ProjectAttributes {
    pub qualified_name: String,
    pub name: String,
    pub anchors: Vec<EntityRef>,
    pub sources: Vec<EntityRef>,
    pub anchor_features: Vec<EntityRef>,
    pub derived_features: Vec<EntityRef>,
    pub tags: HashMap<String, String>,
}

impl From<registry_provider::ProjectAttributes> for ProjectAttributes {
    fn from(v: registry_provider::ProjectAttributes) -> Self {
        Self {
            qualified_name: v.qualified_name,
            name: v.name,
            anchors: v.anchors.into_iter().map(|e| e.into()).collect(),
            sources: v.sources.into_iter().map(|e| e.into()).collect(),
            anchor_features: v.anchor_features.into_iter().map(|e| e.into()).collect(),
            derived_features: v.derived_features.into_iter().map(|e| e.into()).collect(),
            tags: v.tags,
        }
    }
}

impl TryInto<registry_provider::ProjectAttributes> for ProjectAttributes {
    type Error = ApiError;

    fn try_into(self) -> Result<registry_provider::ProjectAttributes, Self::Error> {
        Ok(registry_provider::ProjectAttributes {
            qualified_name: self.qualified_name,
            name: self.name,
            anchors: self
                .anchors
                .into_iter()
                .map(|e| e.try_into())
                .collect::<Result<_, _>>()?,
            sources: self
                .sources
                .into_iter()
                .map(|e| e.try_into())
                .collect::<Result<_, _>>()?,
            anchor_features: self
                .anchor_features
                .into_iter()
                .map(|e| e.try_into())
                .collect::<Result<_, _>>()?,
            derived_features: self
                .derived_features
                .into_iter()
                .map(|e| e.try_into())
                .collect::<Result<_, _>>()?,
            tags: self.tags,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct SourceAttributes {
    pub qualified_name: String,
    pub name: String,
    pub path: String,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub preprocessing: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub event_timestamp_column: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub timestamp_format: Option<String>,
    #[oai(rename = "type")]
    pub type_: String,
    pub tags: HashMap<String, String>,
}

impl From<registry_provider::SourceAttributes> for SourceAttributes {
    fn from(v: registry_provider::SourceAttributes) -> Self {
        Self {
            qualified_name: v.qualified_name,
            name: v.name,
            path: v.path,
            preprocessing: v.preprocessing,
            event_timestamp_column: v.event_timestamp_column,
            timestamp_format: v.timestamp_format,
            type_: v.type_,
            tags: v.tags,
        }
    }
}

impl TryInto<registry_provider::SourceAttributes> for SourceAttributes {
    type Error = ApiError;

    fn try_into(self) -> Result<registry_provider::SourceAttributes, Self::Error> {
        Ok(registry_provider::SourceAttributes {
            qualified_name: self.qualified_name,
            name: self.name,
            path: self.path,
            preprocessing: self.preprocessing,
            event_timestamp_column: self.event_timestamp_column,
            timestamp_format: self.timestamp_format,
            type_: self.type_,
            tags: self.tags,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct AnchorAttributes {
    pub qualified_name: String,
    pub name: String,
    pub features: Vec<EntityRef>,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub source: Option<EntityRef>,
    pub tags: HashMap<String, String>,
}

impl From<registry_provider::AnchorAttributes> for AnchorAttributes {
    fn from(v: registry_provider::AnchorAttributes) -> Self {
        Self {
            qualified_name: v.qualified_name,
            name: v.name,
            features: v.features.into_iter().map(|e| e.into()).collect(),
            source: v.source.map(|e| e.into()),
            tags: v.tags,
        }
    }
}

impl TryInto<registry_provider::AnchorAttributes> for AnchorAttributes {
    type Error = ApiError;

    fn try_into(self) -> Result<registry_provider::AnchorAttributes, Self::Error> {
        Ok(registry_provider::AnchorAttributes {
            qualified_name: self.qualified_name,
            name: self.name,
            features: self
                .features
                .into_iter()
                .map(|e| e.try_into())
                .collect::<Result<_, _>>()?,
            source: self.source.map(|e| e.try_into()).flip()?,
            tags: self.tags,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct AnchorFeatureAttributes {
    pub qualified_name: String,
    pub name: String,
    #[oai(rename = "type")]
    pub type_: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    pub tags: HashMap<String, String>,
}

impl From<registry_provider::AnchorFeatureAttributes> for AnchorFeatureAttributes {
    fn from(v: registry_provider::AnchorFeatureAttributes) -> Self {
        Self {
            qualified_name: v.qualified_name,
            name: v.name,
            type_: v.type_.into(),
            transformation: v.transformation.into(),
            key: v.key.into_iter().map(|e| e.into()).collect(),
            tags: v.tags,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct DerivedFeatureAttributes {
    #[oai(rename = "qualifiedName")]
    pub qualified_name: String,
    pub name: String,
    #[oai(rename = "type")]
    pub type_: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    pub input_anchor_features: Vec<EntityRef>,
    pub input_derived_features: Vec<EntityRef>,
    pub tags: HashMap<String, String>,
}

impl From<registry_provider::DerivedFeatureAttributes> for DerivedFeatureAttributes {
    fn from(v: registry_provider::DerivedFeatureAttributes) -> Self {
        Self {
            qualified_name: v.qualified_name,
            name: v.name,
            type_: v.type_.into(),
            transformation: v.transformation.into(),
            key: v.key.into_iter().map(|e| e.into()).collect(),
            input_anchor_features: v
                .input_anchor_features
                .into_iter()
                .map(|e| e.into())
                .collect(),
            input_derived_features: v
                .input_derived_features
                .into_iter()
                .map(|e| e.into())
                .collect(),
            tags: v.tags,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Union)]
pub enum EntityAttributes {
    Project(ProjectAttributes),
    Source(SourceAttributes),
    Anchor(AnchorAttributes),
    AnchorFeature(AnchorFeatureAttributes),
    DerivedFeature(DerivedFeatureAttributes),
}

impl From<registry_provider::Attributes> for EntityAttributes {
    fn from(v: registry_provider::Attributes) -> Self {
        match v {
            registry_provider::Attributes::AnchorFeature(v) => Self::AnchorFeature(v.into()),
            registry_provider::Attributes::DerivedFeature(v) => Self::DerivedFeature(v.into()),
            registry_provider::Attributes::Anchor(v) => Self::Anchor(v.into()),
            registry_provider::Attributes::Source(v) => Self::Source(v.into()),
            registry_provider::Attributes::Project(v) => Self::Project(v.into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct Entity {
    pub guid: String,
    pub name: String,
    pub qualified_name: String,
    #[oai(rename = "lastModifiedTS")]
    pub last_modified_ts: String,
    #[oai(rename = "typeName")]
    pub entity_type: EntityType,
    pub status: String,
    pub display_text: String,
    pub classification_names: Vec<String>,
    pub meaning_names: Vec<String>,
    pub meanings: Vec<String>,
    pub is_incomplete: bool,
    pub labels: Vec<String>,
    pub attributes: EntityAttributes,
}

impl From<registry_provider::Entity<EntityProperty>> for Entity {
    fn from(v: registry_provider::Entity<EntityProperty>) -> Self {
        Self {
            guid: v.properties.guid.to_string(),
            name: v.name,
            qualified_name: v.qualified_name,
            entity_type: v.entity_type.into(),
            last_modified_ts: v.properties.last_modified_ts,
            status: format!("{:?}", v.properties.status),
            display_text: v.properties.display_text,
            classification_names: v.properties.classification_names,
            meaning_names: v.properties.meaning_names,
            meanings: v.properties.meanings,
            is_incomplete: v.properties.is_incomplete,
            labels: v.properties.labels,
            attributes: v.properties.attributes.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Enum)]
pub enum EdgeType {
    BelongsTo,
    Contains,
    Consumes,
    Produces,
}

impl From<registry_provider::EdgeType> for EdgeType {
    fn from(v: registry_provider::EdgeType) -> Self {
        match v {
            registry_provider::EdgeType::BelongsTo => EdgeType::BelongsTo,
            registry_provider::EdgeType::Contains => EdgeType::Contains,
            registry_provider::EdgeType::Consumes => EdgeType::Consumes,
            registry_provider::EdgeType::Produces => EdgeType::Produces,
        }
    }
}

impl Into<registry_provider::EdgeType> for EdgeType {
    fn into(self) -> registry_provider::EdgeType {
        match self {
            EdgeType::BelongsTo => registry_provider::EdgeType::BelongsTo,
            EdgeType::Contains => registry_provider::EdgeType::Contains,
            EdgeType::Consumes => registry_provider::EdgeType::Consumes,
            EdgeType::Produces => registry_provider::EdgeType::Produces,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Object)]
pub struct Relationship {
    #[oai(rename = "relationshipId")]
    pub guid: String,
    #[oai(rename = "relationshipType")]
    pub edge_type: EdgeType,
    #[oai(rename = "fromEntityId")]
    pub from: String,
    #[oai(rename = "toEntityId")]
    pub to: String,
}

impl From<EdgeProperty> for Relationship {
    fn from(v: EdgeProperty) -> Self {
        Self {
            guid: v.guid.to_string(),
            edge_type: v.edge_type.into(),
            from: v.from.to_string(),
            to: v.to.to_string(),
        }
    }
}

fn parse_uuid(s: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(s).map_err(|_| ApiError::BadRequest(format!("Invalid GUID `{}`", s)))
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Enum)]
pub enum ValueType {
    #[serde(alias = "0")]
    UNSPECIFIED,
    #[serde(rename = "BOOLEAN", alias = "1")]
    #[oai(rename = "BOOLEAN")]
    BOOL,
    #[serde(rename = "INT", alias = "2")]
    #[oai(rename = "INT")]
    INT32,
    #[serde(rename = "LONG", alias = "3")]
    #[oai(rename = "LONG")]
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

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Enum)]
pub enum VectorType {
    TENSOR,
}

impl From<registry_provider::VectorType> for VectorType {
    fn from(_: registry_provider::VectorType) -> Self {
        VectorType::TENSOR
    }
}

impl Into<registry_provider::VectorType> for VectorType {
    fn into(self) -> registry_provider::VectorType {
        registry_provider::VectorType::TENSOR
    }
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Enum)]
pub enum TensorCategory {
    DENSE,
    SPARSE,
}

impl From<registry_provider::TensorCategory> for TensorCategory {
    fn from(v: registry_provider::TensorCategory) -> Self {
        match v {
            registry_provider::TensorCategory::DENSE => TensorCategory::DENSE,
            registry_provider::TensorCategory::SPARSE => TensorCategory::SPARSE,
        }
    }
}

impl Into<registry_provider::TensorCategory> for TensorCategory {
    fn into(self) -> registry_provider::TensorCategory {
        match self {
            TensorCategory::DENSE => registry_provider::TensorCategory::DENSE,
            TensorCategory::SPARSE => registry_provider::TensorCategory::SPARSE,
        }
    }
}

impl From<registry_provider::ValueType> for ValueType {
    fn from(v: registry_provider::ValueType) -> Self {
        match v {
            registry_provider::ValueType::UNSPECIFIED => Self::UNSPECIFIED,
            registry_provider::ValueType::BOOL => Self::BOOL,
            registry_provider::ValueType::INT32 => Self::INT32,
            registry_provider::ValueType::INT64 => Self::INT32,
            registry_provider::ValueType::FLOAT => Self::FLOAT,
            registry_provider::ValueType::DOUBLE => Self::DOUBLE,
            registry_provider::ValueType::STRING => Self::STRING,
            registry_provider::ValueType::BYTES => Self::BYTES,
        }
    }
}

impl Into<registry_provider::ValueType> for ValueType {
    fn into(self) -> registry_provider::ValueType {
        match self {
            ValueType::UNSPECIFIED => registry_provider::ValueType::UNSPECIFIED,
            ValueType::BOOL => registry_provider::ValueType::BOOL,
            ValueType::INT32 => registry_provider::ValueType::INT32,
            ValueType::INT64 => registry_provider::ValueType::INT64,
            ValueType::FLOAT => registry_provider::ValueType::FLOAT,
            ValueType::DOUBLE => registry_provider::ValueType::DOUBLE,
            ValueType::STRING => registry_provider::ValueType::STRING,
            ValueType::BYTES => registry_provider::ValueType::BYTES,
        }
    }
}

impl TryInto<EdgeProperty> for Relationship {
    type Error = ApiError;

    fn try_into(self) -> Result<EdgeProperty, Self::Error> {
        Ok(EdgeProperty {
            guid: parse_uuid(&self.guid)?,
            edge_type: self.edge_type.into(),
            from: parse_uuid(&self.from)?,
            to: parse_uuid(&self.to)?,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct EntityLineage {
    #[serde(rename = "guidEntityMap")]
    pub guid_entity_map: HashMap<String, Entity>,
    pub relations: Vec<Relationship>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[serde(rename_all = "camelCase")]
#[oai(rename_all = "camelCase")]
pub struct ProjectDef {
    pub name: String,
    #[oai(skip)]
    pub qualified_name: String,
    pub tags: HashMap<String, String>,
}

impl TryInto<registry_provider::ProjectDef> for ProjectDef {
    type Error = ApiError;

    fn try_into(self) -> Result<registry_provider::ProjectDef, Self::Error> {
        Ok(registry_provider::ProjectDef {
            qualified_name: self.qualified_name,
            tags: self.tags,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[serde(rename_all = "camelCase")]
pub struct SourceDef {
    pub name: String,
    #[oai(skip)]
    pub qualified_name: String,
    #[serde(rename = "type")]
    pub source_type: String,
    pub path: String,
    pub event_timestamp_column: Option<String>,
    pub timestamp_format: Option<String>,
    pub preprocessing: Option<String>,
    pub tags: HashMap<String, String>,
}

impl TryInto<registry_provider::SourceDef> for SourceDef {
    type Error = ApiError;

    fn try_into(self) -> Result<registry_provider::SourceDef, Self::Error> {
        Ok(registry_provider::SourceDef {
            qualified_name: self.qualified_name,
            name: self.name,
            source_type: self.source_type,
            path: self.path,
            event_timestamp_column: self.event_timestamp_column,
            timestamp_format: self.timestamp_format,
            preprocessing: self.preprocessing,
            tags: self.tags,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[serde(rename_all = "camelCase")]
pub struct AnchorDef {
    pub name: String,
    #[oai(skip)]
    pub qualified_name: String,
    pub source_id: String,
    pub tags: HashMap<String, String>,
}

impl TryInto<registry_provider::AnchorDef> for AnchorDef {
    type Error = ApiError;

    fn try_into(self) -> Result<registry_provider::AnchorDef, Self::Error> {
        Ok(registry_provider::AnchorDef {
            name: self.name,
            qualified_name: self.qualified_name,
            source_id: parse_uuid(&self.source_id)?,
            tags: self.tags,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct FeatureType {
    #[oai(rename = "type")]
    pub type_: VectorType,
    pub tensor_category: TensorCategory,
    pub dimension_type: Vec<ValueType>,
    pub val_type: ValueType,
}

impl TryInto<registry_provider::FeatureType> for FeatureType {
    type Error = ApiError;

    fn try_into(self) -> Result<registry_provider::FeatureType, Self::Error> {
        Ok(registry_provider::FeatureType {
            type_: self.type_.into(),
            tensor_category: self.tensor_category.into(),
            dimension_type: self.dimension_type.into_iter().map(|e| e.into()).collect(),
            val_type: self.val_type.into(),
        })
    }
}

impl From<registry_provider::FeatureType> for FeatureType {
    fn from(v: registry_provider::FeatureType) -> Self {
        Self {
            type_: v.type_.into(),
            tensor_category: v.tensor_category.into(),
            dimension_type: v.dimension_type.into_iter().map(|e| e.into()).collect(),
            val_type: v.val_type.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct TypedKey {
    pub key_column: String,
    pub key_column_type: ValueType,
    #[oai(skip_serializing_if_is_none)]
    pub full_name: Option<String>,
    #[oai(skip_serializing_if_is_none)]
    pub description: Option<String>,
    #[oai(skip_serializing_if_is_none)]
    pub key_column_alias: Option<String>,
}

impl From<registry_provider::TypedKey> for TypedKey {
    fn from(v: registry_provider::TypedKey) -> Self {
        Self {
            key_column: v.key_column,
            key_column_type: v.key_column_type.into(),
            full_name: v.full_name,
            description: v.description,
            key_column_alias: v.key_column_alias,
        }
    }
}

impl TryInto<registry_provider::TypedKey> for TypedKey {
    type Error = ApiError;

    fn try_into(self) -> Result<registry_provider::TypedKey, Self::Error> {
        Ok(registry_provider::TypedKey {
            key_column: self.key_column,
            key_column_type: self.key_column_type.into(),
            full_name: self.full_name,
            description: self.description,
            key_column_alias: self.key_column_alias,
        })
    }
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Enum)]
pub enum Aggregation {
    // No operation
    NOP,
    // Average
    AVG,
    MAX,
    MIN,
    SUM,
    UNION,
    // Element-wise average, typically used in array type value, i.e. 1d dense tensor
    ELEMENTWISE_AVG,
    ELEMENTWISE_MIN,
    ELEMENTWISE_MAX,
    ELEMENTWISE_SUM,
    // Pick the latest value according to its timestamp
    LATEST,
}

impl From<registry_provider::Aggregation> for Aggregation {
    fn from(v: registry_provider::Aggregation) -> Self {
        match v {
            registry_provider::Aggregation::NOP => Aggregation::NOP,
            registry_provider::Aggregation::AVG => Aggregation::AVG,
            registry_provider::Aggregation::MAX => Aggregation::MAX,
            registry_provider::Aggregation::MIN => Aggregation::MIN,
            registry_provider::Aggregation::SUM => Aggregation::SUM,
            registry_provider::Aggregation::UNION => Aggregation::UNION,
            registry_provider::Aggregation::ELEMENTWISE_AVG => Aggregation::ELEMENTWISE_AVG,
            registry_provider::Aggregation::ELEMENTWISE_MIN => Aggregation::ELEMENTWISE_MIN,
            registry_provider::Aggregation::ELEMENTWISE_MAX => Aggregation::ELEMENTWISE_MAX,
            registry_provider::Aggregation::ELEMENTWISE_SUM => Aggregation::ELEMENTWISE_SUM,
            registry_provider::Aggregation::LATEST => Aggregation::LATEST,
        }
    }
}

impl Into<registry_provider::Aggregation> for Aggregation {
    fn into(self) -> registry_provider::Aggregation {
        match self {
            Aggregation::NOP => registry_provider::Aggregation::NOP,
            Aggregation::AVG => registry_provider::Aggregation::AVG,
            Aggregation::MAX => registry_provider::Aggregation::MAX,
            Aggregation::MIN => registry_provider::Aggregation::MIN,
            Aggregation::SUM => registry_provider::Aggregation::SUM,
            Aggregation::UNION => registry_provider::Aggregation::UNION,
            Aggregation::ELEMENTWISE_AVG => registry_provider::Aggregation::ELEMENTWISE_AVG,
            Aggregation::ELEMENTWISE_MIN => registry_provider::Aggregation::ELEMENTWISE_MIN,
            Aggregation::ELEMENTWISE_MAX => registry_provider::Aggregation::ELEMENTWISE_MAX,
            Aggregation::ELEMENTWISE_SUM => registry_provider::Aggregation::ELEMENTWISE_SUM,
            Aggregation::LATEST => registry_provider::Aggregation::LATEST,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct FeatureTransformation {
    #[oai(skip_serializing_if_is_none, default)]
    def_expr: Option<String>,
    #[oai(skip_serializing_if_is_none, default)]
    agg_func: Option<Aggregation>,
    #[oai(skip_serializing_if_is_none, default)]
    window: Option<String>,
    #[oai(skip_serializing_if_is_none, default)]
    group_by: Option<String>,
    #[oai(skip_serializing_if_is_none, default)]
    filter: Option<String>,
    #[oai(skip_serializing_if_is_none, default)]
    limit: Option<u64>,
    #[oai(skip_serializing_if_is_none, default)]
    transform_expr: Option<String>,
    #[oai(skip_serializing_if_is_none, default)]
    name: Option<String>,
}

impl TryInto<registry_provider::FeatureTransformation> for FeatureTransformation {
    type Error = ApiError;

    fn try_into(self) -> Result<registry_provider::FeatureTransformation, Self::Error> {
        Ok(match self.transform_expr {
            Some(s) => registry_provider::FeatureTransformation::Expression { transform_expr: s },
            None => match self.name {
                Some(s) => registry_provider::FeatureTransformation::Udf { name: s },
                None => match self.def_expr {
                    Some(s) => registry_provider::FeatureTransformation::WindowAgg {
                        def_expr: s,
                        agg_func: self.agg_func.map(|a| a.into()),
                        window: self.window,
                        group_by: self.group_by,
                        filter: self.filter,
                        limit: self.limit,
                    },
                    None => {
                        return Err(ApiError::BadRequest(
                            "Invalid feature transformation".to_string(),
                        ))
                    }
                },
            },
        })
    }
}

impl From<registry_provider::FeatureTransformation> for FeatureTransformation {
    fn from(v: registry_provider::FeatureTransformation) -> Self {
        match v {
            registry_provider::FeatureTransformation::Expression { transform_expr } => Self {
                transform_expr: Some(transform_expr),
                ..Default::default()
            },
            registry_provider::FeatureTransformation::WindowAgg {
                def_expr,
                agg_func,
                window,
                group_by,
                filter,
                limit,
            } => Self {
                def_expr: Some(def_expr),
                agg_func: agg_func.map(|a| a.into()),
                window,
                group_by,
                filter,
                limit,
                ..Default::default()
            },
            registry_provider::FeatureTransformation::Udf { name } => Self {
                name: Some(name),
                ..Default::default()
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[serde(rename_all = "camelCase")]
#[oai(rename_all = "camelCase")]
pub struct AnchorFeatureDef {
    pub name: String,
    #[oai(skip)]
    pub qualified_name: String,
    pub feature_type: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    pub tags: HashMap<String, String>,
}

impl TryInto<registry_provider::AnchorFeatureDef> for AnchorFeatureDef {
    type Error = ApiError;

    fn try_into(self) -> Result<registry_provider::AnchorFeatureDef, Self::Error> {
        Ok(registry_provider::AnchorFeatureDef {
            name: self.name,
            qualified_name: self.qualified_name,
            feature_type: self.feature_type.try_into()?,
            transformation: self.transformation.try_into()?,
            key: self
                .key
                .into_iter()
                .map(|e| e.try_into())
                .collect::<Result<_, _>>()?,
            tags: self.tags,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[serde(rename_all = "camelCase")]
#[oai(rename_all = "camelCase")]
pub struct DerivedFeatureDef {
    pub name: String,
    #[oai(skip)]
    pub qualified_name: String,
    pub feature_type: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    #[oai(validator(unique_items))]
    pub input_anchor_features: Vec<String>,
    #[oai(validator(unique_items))]
    pub input_derived_features: Vec<String>,
    pub tags: HashMap<String, String>,
}

impl TryInto<registry_provider::DerivedFeatureDef> for DerivedFeatureDef {
    type Error = ApiError;

    fn try_into(self) -> Result<registry_provider::DerivedFeatureDef, Self::Error> {
        Ok(registry_provider::DerivedFeatureDef {
            name: self.name,
            qualified_name: self.qualified_name,
            feature_type: self.feature_type.try_into()?,
            transformation: self.transformation.try_into()?,
            key: self
                .key
                .into_iter()
                .map(|e| e.try_into())
                .collect::<Result<_, _>>()?,
            input_anchor_features: self
                .input_anchor_features
                .into_iter()
                .map(|s| parse_uuid(&s))
                .collect::<Result<_, _>>()?,
            input_derived_features: self
                .input_derived_features
                .into_iter()
                .map(|s| parse_uuid(&s))
                .collect::<Result<_, _>>()?,
            tags: self.tags,
        })
    }
}

#[derive(Clone, Debug, Serialize, Object)]
pub struct CreationResponse {
    pub guid: String,
}

impl TryInto<Uuid> for CreationResponse {
    type Error = ApiError;

    fn try_into(self) -> Result<Uuid, Self::Error> {
        parse_uuid(&self.guid)
    }
}

impl From<Uuid> for CreationResponse {
    fn from(id: Uuid) -> Self {
        Self {
            guid: id.to_string(),
        }
    }
}
