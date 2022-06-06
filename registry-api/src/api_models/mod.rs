use std::collections::HashMap;

use poem_openapi::{Enum, Object};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ApiError;

mod attributes;
mod edge;
mod entity;

pub use attributes::*;
pub use edge::*;
pub use entity::*;

fn parse_uuid(s: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(s).map_err(|_| ApiError::BadRequest(format!("Invalid GUID `{}`", s)))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[serde(rename_all = "camelCase")]
#[oai(rename_all = "camelCase")]
pub struct ProjectDef {
    pub name: String,
    #[oai(skip)]
    pub qualified_name: String,
    #[oai(default)]
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
    #[oai(default)]
    pub event_timestamp_column: Option<String>,
    #[oai(default)]
    pub timestamp_format: Option<String>,
    #[oai(default)]
    pub preprocessing: Option<String>,
    #[oai(default)]
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
    #[oai(default)]
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
    #[oai(default)]
    pub tensor_category: TensorCategory,
    #[oai(default)]
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
    #[oai(skip_serializing_if_is_none, default)]
    pub full_name: Option<String>,
    #[oai(skip_serializing_if_is_none, default)]
    pub description: Option<String>,
    #[oai(skip_serializing_if_is_none, default)]
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
    #[oai(default)]
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
    #[oai(validator(unique_items), default)]
    pub input_anchor_features: Vec<String>,
    #[oai(validator(unique_items), default)]
    pub input_derived_features: Vec<String>,
    #[oai(default)]
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
