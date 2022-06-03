use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use log::debug;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ANCHOR_FEATURE_TYPE, SOURCE_TYPE, ANCHOR_TYPE};

fn is_default<T>(v: &T) -> bool
where
    T: Default + Eq,
{
    v == &T::default()
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

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FeatureTransformation {
    Expression {
        transform_expr: String,
    },
    WindowAgg {
        def_expr: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        agg_func: Option<Aggregation>,
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
    pub(crate) fn add_input_anchor_feature(&mut self, id: Uuid, attr: &AnchorFeatureAttributes) {
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

    pub(crate) fn add_input_derived_feature(&mut self, id: Uuid, attr: &DerivedFeatureAttributes) {
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

    pub(crate) fn remove_input_anchor_feature(&mut self, id: Uuid, attr: &AnchorFeatureAttributes) {
        debug!(
            "Removing anchor feature '{}' from derived feature '{}' input",
            attr.name, self.name
        );
        self.input_anchor_features.remove(&EntityRef {
            guid: id,
            ..Default::default()
        });
    }

    pub(crate) fn remove_input_derived_feature(&mut self, id: Uuid, attr: &DerivedFeatureAttributes) {
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
    pub(crate) fn add_anchor_feature(&mut self, id: Uuid, attr: &AnchorFeatureAttributes) {
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

    pub(crate) fn remove_anchor_feature(&mut self, id: Uuid, attr: &AnchorFeatureAttributes) {
        debug!(
            "Removing anchor feature '{}' from anchor '{}'",
            attr.name, self.name
        );
        self.features.remove(&EntityRef {
            guid: id,
            ..Default::default()
        });
    }

    pub(crate) fn set_source(&mut self, id: Uuid, attr: &SourceAttributes) {
        debug!("Setting source '{}' for anchor '{}'", attr.name, self.name);
        self.source = Some(EntityRef {
            guid: id,
            type_name: SOURCE_TYPE.to_string(),
            unique_attributes: [("qualifiedName".to_string(), attr.qualified_name.to_owned())]
                .into_iter()
                .collect(),
        });
    }

    pub(crate) fn reset_source(&mut self, _id: Uuid, attr: &SourceAttributes) {
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
    pub(crate) fn add_anchor(&mut self, id: Uuid, attr: &AnchorAttributes) {
        debug!("Adding anchor '{}' into project '{}'", attr.name, self.name);
        self.anchors.insert(EntityRef {
            guid: id,
            type_name: ANCHOR_TYPE.to_string(),
            unique_attributes: [("qualifiedName".to_string(), attr.qualified_name.to_owned())]
                .into_iter()
                .collect(),
        });
    }

    pub(crate) fn add_source(&mut self, id: Uuid, attr: &SourceAttributes) {
        debug!("Adding source '{}' into project '{}'", attr.name, self.name);
        self.sources.insert(EntityRef {
            guid: id,
            type_name: SOURCE_TYPE.to_string(),
            unique_attributes: [("qualifiedName".to_string(), attr.qualified_name.to_owned())]
                .into_iter()
                .collect(),
        });
    }

    pub(crate) fn add_anchor_feature(&mut self, id: Uuid, attr: &AnchorFeatureAttributes) {
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

    pub(crate) fn add_derived_feature(&mut self, id: Uuid, attr: &DerivedFeatureAttributes) {
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

    pub(crate) fn remove_anchor(&mut self, id: Uuid, attr: &AnchorAttributes) {
        debug!(
            "Removing anchor '{}' from project '{}'",
            attr.name, self.name
        );
        self.anchors.remove(&EntityRef {
            guid: id,
            ..Default::default()
        });
    }

    pub(crate) fn remove_source(&mut self, id: Uuid, attr: &SourceAttributes) {
        debug!(
            "Removing source '{}' from project '{}'",
            attr.name, self.name
        );
        self.sources.remove(&EntityRef {
            guid: id,
            ..Default::default()
        });
    }

    pub(crate) fn remove_anchor_feature(&mut self, id: Uuid, attr: &AnchorFeatureAttributes) {
        debug!(
            "Removing anchor feature '{}' from project '{}'",
            attr.name, self.name
        );
        self.anchor_features.remove(&EntityRef {
            guid: id,
            ..Default::default()
        });
    }

    pub(crate) fn remove_derived_feature(&mut self, id: Uuid, attr: &DerivedFeatureAttributes) {
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
