use std::collections::HashMap;

use common_utils::FlippedOptionResult;
use poem_openapi::{Enum, Object, Union};
use registry_provider::EdgeProperty;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;

use super::{parse_uuid, EntityRef, FeatureTransformation, FeatureType, Relationship, TypedKey};

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

impl Default for TensorCategory {
    fn default() -> Self {
        Self::DENSE
    }
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
            edge_type: self.edge_type.into(),
            from: parse_uuid(&self.from)?,
            to: parse_uuid(&self.to)?,
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
