use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AnchorAttributes, AnchorDef, AnchorFeatureAttributes, AnchorFeatureDef, Attributes, ContentEq,
    DerivedFeatureAttributes, DerivedFeatureDef, EdgeType, Entity, EntityPropMutator, EntityRef,
    EntityType, ProjectAttributes, ProjectDef, RegistryError, SourceAttributes, SourceDef,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EntityStatus {
    Active,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

/**
 * Only attributes are worthy to compare
 */
impl ContentEq for EntityProperty {
    fn content_eq(&self, other: &Self) -> bool {
        self.attributes.content_eq(&other.attributes)
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
                anchors: Default::default(),
                sources: Default::default(),
                anchor_features: Default::default(),
                derived_features: Default::default(),
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
                features: Default::default(),
                source: Some(EntityRef {
                    guid: definition.source_id,
                    type_name: Default::default(),
                    unique_attributes: Default::default(),
                }), // Will be set later by `connect`
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
                input_anchor_features: definition
                    .input_anchor_features
                    .iter()
                    .map(|id| EntityRef {
                        guid: id.to_owned(),
                        ..Default::default()
                    })
                    .collect(),
                input_derived_features: definition
                    .input_derived_features
                    .iter()
                    .map(|id| EntityRef {
                        guid: id.to_owned(),
                        ..Default::default()
                    })
                    .collect(),
                tags: definition.tags.to_owned(),
            }),
        })
    }

    fn clear(&mut self) {
        self.attributes.clear()
    }

    fn connect(
        from: &mut Entity<Self>,
        from_id: Uuid,
        to: &mut Entity<Self>,
        to_id: Uuid,
        edge_type: EdgeType,
    ) {
        if edge_type == EdgeType::Contains {
            to.containers.insert(from_id);
            match (
                &mut from.properties.attributes,
                &mut to.properties.attributes,
            ) {
                (Attributes::Anchor(from), Attributes::AnchorFeature(to)) => {
                    from.add_anchor_feature(to_id, to);
                }
                (Attributes::Project(from), Attributes::AnchorFeature(to)) => {
                    from.add_anchor_feature(to_id, to);
                }
                (Attributes::Project(from), Attributes::DerivedFeature(to)) => {
                    from.add_derived_feature(to_id, to);
                }
                (Attributes::Project(from), Attributes::Anchor(to)) => {
                    from.add_anchor(to_id, to);
                }
                (Attributes::Project(from), Attributes::Source(to)) => {
                    from.add_source(to_id, to);
                }
                _ => {}
            }
        } else if edge_type == EdgeType::Consumes {
            match (
                &mut from.properties.attributes,
                &mut to.properties.attributes,
            ) {
                (Attributes::DerivedFeature(from), Attributes::AnchorFeature(to)) => {
                    from.add_input_anchor_feature(to_id, to);
                }
                (Attributes::DerivedFeature(from), Attributes::DerivedFeature(to)) => {
                    from.add_input_derived_feature(to_id, to);
                }
                (Attributes::Anchor(from), Attributes::Source(to)) => {
                    from.set_source(to_id, to);
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
    ) {
        if edge_type == EdgeType::Contains {
            to.containers.remove(&from_id);
            match (
                &mut from.properties.attributes,
                &mut to.properties.attributes,
            ) {
                (Attributes::Anchor(from), Attributes::AnchorFeature(to)) => {
                    from.remove_anchor_feature(to_id, to);
                }
                (Attributes::Project(from), Attributes::AnchorFeature(to)) => {
                    from.remove_anchor_feature(to_id, to);
                }
                (Attributes::Project(from), Attributes::DerivedFeature(to)) => {
                    from.remove_derived_feature(to_id, to);
                }
                (Attributes::Project(from), Attributes::Anchor(to)) => {
                    from.remove_anchor(to_id, to);
                }
                (Attributes::Project(from), Attributes::Source(to)) => {
                    from.remove_source(to_id, to);
                }
                _ => {}
            }
        } else if edge_type == EdgeType::Consumes {
            match (
                &mut from.properties.attributes,
                &mut to.properties.attributes,
            ) {
                (Attributes::DerivedFeature(from), Attributes::AnchorFeature(to)) => {
                    from.remove_input_anchor_feature(to_id, to);
                }
                (Attributes::DerivedFeature(from), Attributes::DerivedFeature(to)) => {
                    from.remove_input_derived_feature(to_id, to);
                }
                (Attributes::Anchor(from), Attributes::Source(to)) => {
                    from.reset_source(to_id, to);
                }
                _ => {}
            }
        }
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
