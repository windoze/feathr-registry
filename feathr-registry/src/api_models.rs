use std::collections::HashMap;

use common_utils::FlippedOptionResult;
use poem_openapi::{Object, Union};
use registry_provider::{
    AnchorAttributes, AnchorFeatureAttributes, DerivedFeatureAttributes, EdgeProperty, EdgeType,
    Entity, EntityProperty, EntityRef, FeatureTransformation, FeatureType, ProjectAttributes,
    SourceAttributes, TensorCategory, TypedKey, ValueType, VectorType,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ApiError;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[serde(rename_all = "camelCase")]
#[oai(rename_all = "camelCase")]
pub struct EntityResponseItem {
    pub id: String,
    pub qualified_name: String,
    pub name: String,
}

impl<Prop> From<Entity<Prop>> for EntityResponseItem
where
    Prop: Clone + std::fmt::Debug + PartialEq + Eq,
{
    fn from(e: Entity<Prop>) -> Self {
        Self {
            id: e.id.to_string(),
            qualified_name: e.qualified_name,
            name: e.name,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
pub struct EntitiesResponse {
    pub entities: Vec<EntityResponseItem>,
}

impl FromIterator<Entity<EntityProperty>> for EntitiesResponse {
    fn from_iter<T: IntoIterator<Item = Entity<EntityProperty>>>(iter: T) -> Self {
        Self {
            entities: iter.into_iter().map(|e| e.into()).collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SearchParams {
    pub keyword: String,
    pub size: Option<usize>,
    pub offset: Option<usize>,
}

impl SearchParams {
    pub fn is_empty(&self) -> bool {
        self.keyword.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct EntityRefResponse {
    guid: String,
    type_name: String,
    unique_attributes: HashMap<String, String>,
}

impl From<EntityRef> for EntityRefResponse {
    fn from(v: EntityRef) -> Self {
        Self {
            guid: v.guid.to_string(),
            type_name: v.type_name,
            unique_attributes: v.unique_attributes,
        }
    }
}

impl TryInto<EntityRef> for EntityRefResponse {
    type Error = ApiError;

    fn try_into(self) -> Result<EntityRef, Self::Error> {
        Ok(EntityRef {
            guid: parse_uuid(&self.guid)?,
            type_name: self.type_name,
            unique_attributes: self.unique_attributes,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct ProjectAttributesResponse {
    pub qualified_name: String,
    pub name: String,
    pub anchors: Vec<EntityRefResponse>,
    pub sources: Vec<EntityRefResponse>,
    pub anchor_features: Vec<EntityRefResponse>,
    pub derived_features: Vec<EntityRefResponse>,
    pub tags: HashMap<String, String>,
}

impl From<ProjectAttributes> for ProjectAttributesResponse {
    fn from(v: ProjectAttributes) -> Self {
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

impl TryInto<ProjectAttributes> for ProjectAttributesResponse {
    type Error = ApiError;

    fn try_into(self) -> Result<ProjectAttributes, Self::Error> {
        Ok(ProjectAttributes {
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

#[derive(Clone, Debug, Serialize, Deserialize, Object)]
pub struct SourceAttributesResponse {
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

impl From<SourceAttributes> for SourceAttributesResponse {
    fn from(v: SourceAttributes) -> Self {
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

impl TryInto<SourceAttributes> for SourceAttributesResponse {
    type Error = ApiError;

    fn try_into(self) -> Result<SourceAttributes, Self::Error> {
        Ok(SourceAttributes {
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

#[derive(Clone, Debug, Serialize, Deserialize, Object)]
pub struct AnchorAttributesResponse {
    #[oai(rename = "qualifiedName")]
    pub qualified_name: String,
    pub name: String,
    pub features: Vec<EntityRefResponse>,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub source: Option<EntityRefResponse>,
    pub tags: HashMap<String, String>,
}

impl From<AnchorAttributes> for AnchorAttributesResponse {
    fn from(v: AnchorAttributes) -> Self {
        Self {
            qualified_name: v.qualified_name,
            name: v.name,
            features: v.features.into_iter().map(|e| e.into()).collect(),
            source: v.source.map(|e| e.into()),
            tags: v.tags,
        }
    }
}

impl TryInto<AnchorAttributes> for AnchorAttributesResponse {
    type Error = ApiError;

    fn try_into(self) -> Result<AnchorAttributes, Self::Error> {
        Ok(AnchorAttributes {
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

#[derive(Clone, Debug, Serialize, Deserialize, Object)]
pub struct AnchorFeatureAttributesResponse {
    pub qualified_name: String,
    pub name: String,
    #[oai(rename = "type")]
    pub type_: FeatureTypeDef,
    pub transformation: FeatureTransformationDef,
    pub key: Vec<TypedKeyDef>,
    pub tags: HashMap<String, String>,
}

impl From<AnchorFeatureAttributes> for AnchorFeatureAttributesResponse {
    fn from(v: AnchorFeatureAttributes) -> Self {
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

#[derive(Clone, Debug, Serialize, Deserialize, Object)]
pub struct DerivedFeatureAttributesResponse {
    #[oai(rename = "qualifiedName")]
    pub qualified_name: String,
    pub name: String,
    #[oai(rename = "type")]
    pub type_: FeatureTypeDef,
    pub transformation: FeatureTransformationDef,
    pub key: Vec<TypedKeyDef>,
    pub input_anchor_features: Vec<EntityRefResponse>,
    pub input_derived_features: Vec<EntityRefResponse>,
    pub tags: HashMap<String, String>,
}

impl From<DerivedFeatureAttributes> for DerivedFeatureAttributesResponse {
    fn from(v: DerivedFeatureAttributes) -> Self {
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

#[derive(Clone, Debug, Serialize, Deserialize, Union)]
pub enum EntityAttributes {
    Project(ProjectAttributesResponse),
    Source(SourceAttributesResponse),
    Anchor(AnchorAttributesResponse),
    AnchorFeature(AnchorFeatureAttributesResponse),
    DerivedFeature(DerivedFeatureAttributesResponse),
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

#[derive(Clone, Debug, Serialize, Deserialize, Object)]
pub struct EntityResponse {
    pub guid: String,
    #[oai(rename = "lastModifiedTS")]
    pub last_modified_ts: String,
    pub status: String,
    pub display_text: String,
    pub classification_names: Vec<String>,
    pub meaning_names: Vec<String>,
    pub meanings: Vec<String>,
    pub is_incomplete: bool,
    pub labels: Vec<String>,
    pub attributes: EntityAttributes,
}

impl From<EntityProperty> for EntityResponse {
    fn from(v: EntityProperty) -> Self {
        Self {
            guid: v.guid.to_string(),
            last_modified_ts: v.last_modified_ts,
            status: format!("{:?}", v.status),
            display_text: v.display_text,
            classification_names: v.classification_names,
            meaning_names: v.meaning_names,
            meanings: v.meanings,
            is_incomplete: v.is_incomplete,
            labels: v.labels,
            attributes: v.attributes.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Object)]
pub struct EdgeResponse {
    #[oai(rename = "relationshipId")]
    pub guid: String,
    #[oai(rename = "relationshipType")]
    pub edge_type: String,
    #[oai(rename = "fromEntityId")]
    pub from: String,
    #[oai(rename = "toEntityId")]
    pub to: String,
}

impl From<EdgeProperty> for EdgeResponse {
    fn from(v: EdgeProperty) -> Self {
        Self {
            guid: v.guid.to_string(),
            edge_type: format!("{:?}", v.edge_type),
            from: v.from.to_string(),
            to: v.to.to_string(),
        }
    }
}

fn parse_uuid(s: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(s).map_err(|_| ApiError::BadRequest(format!("Invalid GUID `{}`", s)))
}

fn parse_edge_type(s: &str) -> Result<EdgeType, ApiError> {
    match s {
        "Contains" => Ok(EdgeType::Contains),
        "BelongsTo" => Ok(EdgeType::BelongsTo),
        "Consumes" => Ok(EdgeType::Consumes),
        "Produces" => Ok(EdgeType::Produces),
        _ => Err(ApiError::BadRequest(format!("Invalid EdgeType `{}`", s))),
    }
}

fn parse_vector_type(s: &str) -> Result<VectorType, ApiError> {
    match s {
        "TENSOR" => Ok(VectorType::TENSOR),
        _ => Err(ApiError::BadRequest(format!("Invalid VectorType `{}`", s))),
    }
}

fn parse_tensor_category(s: &str) -> Result<TensorCategory, ApiError> {
    match s {
        "DENSE" => Ok(TensorCategory::DENSE),
        "SPARSE" => Ok(TensorCategory::SPARSE),
        _ => Err(ApiError::BadRequest(format!(
            "Invalid TensorCategory `{}`",
            s
        ))),
    }
}

fn parse_value_type(s: &str) -> Result<ValueType, ApiError> {
    match s {
        "UNSPECIFIED" | "0" => Ok(ValueType::BOOL),
        "BOOL" | "BOOLEAN" | "1" => Ok(ValueType::BOOL),
        "INT32" | "INT" | "2" => Ok(ValueType::INT32),
        "INT64" | "LONG" | "3" => Ok(ValueType::INT64),
        "FLOAT" | "4" => Ok(ValueType::FLOAT),
        "DOUBLE" | "5" => Ok(ValueType::DOUBLE),
        "STRING" | "6" => Ok(ValueType::STRING),
        "BYTES" | "7" => Ok(ValueType::BYTES),
        _ => Err(ApiError::BadRequest(format!("Invalid ValueType `{}`", s))),
    }
}

impl TryInto<EdgeProperty> for EdgeResponse {
    type Error = ApiError;

    fn try_into(self) -> Result<EdgeProperty, Self::Error> {
        Ok(EdgeProperty {
            guid: parse_uuid(&self.guid)?,
            edge_type: parse_edge_type(&self.edge_type)?,
            from: parse_uuid(&self.from)?,
            to: parse_uuid(&self.to)?,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Object)]
pub struct FeatureLineageResponse {
    #[serde(rename = "guidEntityMap")]
    pub guid_entity_map: HashMap<String, EntityResponse>,
    pub relations: Vec<EdgeResponse>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[serde(rename_all = "camelCase")]
pub struct ProjectDef {
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnchorDef {
    pub name: String,
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct FeatureTypeDef {
    #[oai(rename = "type")]
    pub type_: String,
    pub tensor_category: String,
    pub dimension_type: Vec<String>,
    pub val_type: String,
}

impl TryInto<registry_provider::FeatureType> for FeatureTypeDef {
    type Error = ApiError;

    fn try_into(self) -> Result<registry_provider::FeatureType, Self::Error> {
        Ok(registry_provider::FeatureType {
            type_: parse_vector_type(&self.type_)?,
            tensor_category: parse_tensor_category(&self.tensor_category)?,
            dimension_type: self
                .dimension_type
                .into_iter()
                .map(|e| parse_value_type(&e))
                .collect::<Result<_, _>>()?,
            val_type: parse_value_type(&self.val_type)?,
        })
    }
}

impl From<FeatureType> for FeatureTypeDef {
    fn from(v: FeatureType) -> Self {
        Self {
            type_: format!("{:?}", v.type_),
            tensor_category: format!("{:?}", v.tensor_category),
            dimension_type: v
                .dimension_type
                .into_iter()
                .map(|e| format!("{:?}", e))
                .collect(),
            val_type: format!("{:?}", v.val_type),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Object)]
pub struct TypedKeyDef {
    pub key_column: String,
    pub key_column_type: String,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub full_name: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub key_column_alias: Option<String>,
}

impl From<TypedKey> for TypedKeyDef {
    fn from(v: TypedKey) -> Self {
        Self {
            key_column: v.key_column,
            key_column_type: format!("{:?}", v.key_column_type),
            full_name: v.full_name,
            description: v.description,
            key_column_alias: v.key_column_alias,
        }
    }
}

impl TryInto<TypedKey> for TypedKeyDef {
    type Error = ApiError;

    fn try_into(self) -> Result<TypedKey, Self::Error> {
        Ok(TypedKey {
            key_column: self.key_column,
            key_column_type: parse_value_type(&self.key_column_type)?,
            full_name: self.full_name,
            description: self.description,
            key_column_alias: self.key_column_alias,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, Object)]
pub struct FeatureTransformationDef {
    #[oai(skip_serializing_if = "Option::is_none")]
    def_expr: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    agg_func: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    window: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    group_by: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    filter: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    limit: Option<u64>,
    #[oai(skip_serializing_if = "Option::is_none")]
    transform_expr: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

impl TryInto<FeatureTransformation> for FeatureTransformationDef {
    type Error = ApiError;

    fn try_into(self) -> Result<FeatureTransformation, Self::Error> {
        Ok(match self.transform_expr {
            Some(s) => FeatureTransformation::Expression { transform_expr: s },
            None => match self.name {
                Some(s) => FeatureTransformation::Udf { name: s },
                None => match self.def_expr {
                    Some(s) => FeatureTransformation::WindowAgg {
                        def_expr: s,
                        agg_func: self.agg_func,
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

impl From<FeatureTransformation> for FeatureTransformationDef {
    fn from(v: FeatureTransformation) -> Self {
        match v {
            FeatureTransformation::Expression { transform_expr } => Self {
                transform_expr: Some(transform_expr),
                ..Default::default()
            },
            FeatureTransformation::WindowAgg {
                def_expr,
                agg_func,
                window,
                group_by,
                filter,
                limit,
            } => Self {
                def_expr: Some(def_expr),
                agg_func,
                window,
                group_by,
                filter,
                limit,
                ..Default::default()
            },
            FeatureTransformation::Udf { name } => Self {
                name: Some(name),
                ..Default::default()
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[serde(rename_all = "camelCase")]
pub struct AnchorFeatureDef {
    pub name: String,
    pub qualified_name: String,
    pub feature_type: FeatureTypeDef,
    pub transformation: FeatureTransformationDef,
    pub key: Vec<TypedKeyDef>,
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
pub struct DerivedFeatureDef {
    pub name: String,
    pub qualified_name: String,
    pub feature_type: FeatureTypeDef,
    pub transformation: FeatureTransformationDef,
    pub key: Vec<TypedKeyDef>,
    pub input_anchor_features: Vec<String>,
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
