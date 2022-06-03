use std::collections::HashMap;

use poem_openapi::{Enum, Object};
use registry_provider::EntityProperty;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;

use super::{parse_uuid, EntityAttributes, Relationship};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Enum)]
pub enum EntityType {
    #[oai(rename = "unknown")]
    Unknown,

    #[oai(rename = "feathr_workspace_v1")]
    Project,
    #[oai(rename = "feathr_source_v1")]
    Source,
    #[oai(rename = "feathr_anchor_v1")]
    Anchor,
    #[oai(rename = "feathr_anchor_feature_v1")]
    AnchorFeature,
    #[oai(rename = "feathr_derived_feature_v1")]
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

#[derive(Clone, Debug, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct EntityLineage {
    #[serde(rename = "guidEntityMap")]
    pub guid_entity_map: HashMap<String, Entity>,
    pub relations: Vec<Relationship>,
}
