use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AnchorAttributes, AnchorDef, AnchorFeatureAttributes, AnchorFeatureDef, Attributes,
    DerivedFeatureAttributes, DerivedFeatureDef, Entity, EntityPropMutator,
    EntityType, ProjectAttributes, ProjectDef, RegistryError, SourceAttributes, SourceDef,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EntityStatus {
    Active,
}

#[derive(Clone, Debug, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityProperty {
    pub guid: Uuid,
    #[serde(rename = "lastModifiedTS")]
    pub last_modified_ts: String,
    pub status: EntityStatus,
    pub display_text: String,
    pub labels: Vec<String>,
    #[serde(flatten)]
    pub attributes: Attributes,
}

impl PartialEq for EntityProperty {
    fn eq(&self, other: &Self) -> bool {
        self.attributes == other.attributes
    }
}

impl EntityPropMutator for EntityProperty {
    fn new_project(definition: &ProjectDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: definition.id,
            last_modified_ts: chrono::Utc::now().timestamp().to_string(),
            status: EntityStatus::Active,
            display_text: definition.qualified_name.to_owned(),
            labels: Default::default(),
            attributes: Attributes::Project(ProjectAttributes {
                qualified_name: definition.qualified_name.to_owned(),
                name: definition.qualified_name.to_owned(),
                tags: definition.tags.to_owned(),
            }),
        })
    }
    fn new_source(definition: &SourceDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: definition.id,
            last_modified_ts: chrono::Utc::now().timestamp().to_string(),
            status: EntityStatus::Active,
            display_text: definition.name.to_owned(),
            labels: Default::default(),
            attributes: Attributes::Source(SourceAttributes {
                qualified_name: definition.qualified_name.to_owned(),
                name: definition.name.to_owned(),
                path: definition.path.to_owned(),
                url: definition.url.to_owned(),
                dbtable: definition.dbtable.to_owned(),
                query: definition.query.to_owned(),
                auth: definition.auth.to_owned(),
                preprocessing: definition.preprocessing.to_owned(),
                event_timestamp_column: definition.event_timestamp_column.to_owned(),
                timestamp_format: definition.timestamp_format.to_owned(),
                type_: definition.source_type.to_owned(),
                tags: definition.tags.to_owned(),
            }),
        })
    }
    fn new_anchor(definition: &AnchorDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: definition.id,
            last_modified_ts: chrono::Utc::now().timestamp().to_string(),
            status: EntityStatus::Active,
            display_text: definition.name.to_owned(),
            labels: Default::default(),
            attributes: Attributes::Anchor(AnchorAttributes {
                qualified_name: definition.qualified_name.to_owned(),
                name: definition.name.to_owned(),
                tags: definition.tags.to_owned(),
            }),
        })
    }
    fn new_anchor_feature(definition: &AnchorFeatureDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: definition.id,
            last_modified_ts: chrono::Utc::now().timestamp().to_string(),
            status: EntityStatus::Active,
            display_text: definition.name.to_owned(),
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
    fn new_derived_feature(definition: &DerivedFeatureDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: definition.id,
            last_modified_ts: chrono::Utc::now().timestamp().to_string(),
            status: EntityStatus::Active,
            display_text: definition.name.to_owned(),
            labels: Default::default(),
            attributes: Attributes::DerivedFeature(DerivedFeatureAttributes {
                qualified_name: definition.qualified_name.to_owned(),
                name: definition.name.to_owned(),
                type_: definition.feature_type.to_owned(),
                transformation: definition.transformation.to_owned(),
                key: definition.key.to_owned(),
                tags: definition.tags.to_owned(),
            }),
        })
    }
}

impl From<EntityProperty> for Entity<EntityProperty> {
    fn from(v: EntityProperty) -> Self {
        match &v.attributes {
            Attributes::AnchorFeature(AnchorFeatureAttributes {
                name,
                qualified_name,
                ..
            }) => Entity::<EntityProperty> {
                id: v.guid,
                entity_type: EntityType::AnchorFeature,
                name: name.to_owned(),
                qualified_name: qualified_name.to_owned(),
                containers: Default::default(),
                properties: v,
            },
            Attributes::DerivedFeature(DerivedFeatureAttributes {
                name,
                qualified_name,
                ..
            }) => Entity::<EntityProperty> {
                id: v.guid,
                entity_type: EntityType::DerivedFeature,
                name: name.to_owned(),
                qualified_name: qualified_name.to_owned(),
                containers: Default::default(),
                properties: v,
            },
            Attributes::Anchor(AnchorAttributes {
                name,
                qualified_name,
                ..
            }) => Entity::<EntityProperty> {
                id: v.guid,
                entity_type: EntityType::Anchor,
                name: name.to_owned(),
                qualified_name: qualified_name.to_owned(),
                containers: Default::default(),
                properties: v,
            },
            Attributes::Source(SourceAttributes {
                name,
                qualified_name,
                ..
            }) => Entity::<EntityProperty> {
                id: v.guid,
                entity_type: EntityType::Source,
                name: name.to_owned(),
                qualified_name: qualified_name.to_owned(),
                containers: Default::default(),
                properties: v,
            },
            Attributes::Project(ProjectAttributes {
                name,
                qualified_name,
                ..
            }) => Entity::<EntityProperty> {
                id: v.guid,
                entity_type: EntityType::Project,
                name: name.to_owned(),
                qualified_name: qualified_name.to_owned(),
                containers: Default::default(),
                properties: v,
            },
        }
    }
}
