use std::{fmt::Debug, collections::HashSet};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use serde::de::{Visitor, SeqAccess, self, MapAccess};
use serde::ser::SerializeStruct;
use serde::{Serialize, Deserialize};
use uuid::Uuid;

use crate::{ProjectDef, RegistryError, SourceDef, AnchorDef, AnchorFeatureDef, DerivedFeatureDef, EdgeType};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityType {
    Unknown,

    Project,
    Source,
    Anchor,
    AnchorFeature,
    DerivedFeature,
}

impl EntityType {
    pub fn is_entry_point(self) -> bool {
        match self {
            EntityType::Project => true,
            _ => false,
        }
    }
}

impl Default for EntityType {
    fn default() -> Self {
        Self::Unknown
    }
}


#[derive(Clone, Debug, Eq)]
pub struct Entity<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    pub id: Uuid,
    pub entity_type: EntityType,
    pub name: String,
    pub qualified_name: String,
    pub containers: HashSet<Uuid>,
    pub properties: Prop,
}

impl<Prop> PartialEq for Entity<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<Prop> Hash for Entity<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<Prop> Serialize for Entity<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq + Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut entity = serializer.serialize_struct("Entity", 6)?;
        entity.serialize_field("id", &self.id)?;
        entity.serialize_field("entity_type", &self.entity_type)?;
        entity.serialize_field("name", &self.name)?;
        entity.serialize_field("qualified_name", &self.qualified_name)?;
        entity.serialize_field("containers", &self.containers)?;
        entity.serialize_field("properties", &self.properties)?;
        entity.end()
    }
}

impl<'de, Prop> Deserialize<'de> for Entity<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            Id,
            EntityType,
            Name,
            QualifiedName,
            Containers,
            Properties,
        }
        struct EntityVisitor<T> {
            _t: std::marker::PhantomData<T>,
        }

        impl<'de, Prop> Visitor<'de> for EntityVisitor<Prop>
        where
            Prop: Clone + Debug + PartialEq + Eq + Deserialize<'de>,
        {
            type Value = Entity<Prop>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct Entity")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Entity<Prop>, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let id = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let entity_type = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let name = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let qualified_name = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(3, &self))?;
                let containers = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(4, &self))?;
                let properties = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(5, &self))?;
                Ok(Entity::<Prop> {
                    id,
                    entity_type,
                    name,
                    qualified_name,
                    containers,
                    properties,
                })
            }

            fn visit_map<V>(self, mut map: V) -> Result<Entity<Prop>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut id = None;
                let mut entity_type = None;
                let mut name = None;
                let mut qualified_name = None;
                let mut containers = None;
                let mut properties = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Id => {
                            if id.is_some() {
                                return Err(de::Error::duplicate_field("id"));
                            }
                            id = Some(map.next_value()?);
                        }
                        Field::EntityType => {
                            if entity_type.is_some() {
                                return Err(de::Error::duplicate_field("entity_type"));
                            }
                            entity_type = Some(map.next_value()?);
                        }
                        Field::Name => {
                            if name.is_some() {
                                return Err(de::Error::duplicate_field("name"));
                            }
                            name = Some(map.next_value()?);
                        }
                        Field::QualifiedName => {
                            if qualified_name.is_some() {
                                return Err(de::Error::duplicate_field("qualified_name"));
                            }
                            qualified_name = Some(map.next_value()?);
                        }
                        Field::Containers => {
                            if containers.is_some() {
                                return Err(de::Error::duplicate_field("containers"));
                            }
                            containers = Some(map.next_value()?);
                        }
                        Field::Properties => {
                            if properties.is_some() {
                                return Err(de::Error::duplicate_field("properties"));
                            }
                            properties = Some(map.next_value()?);
                        }
                    }
                }
                let id = id.ok_or_else(|| de::Error::missing_field("id"))?;
                let entity_type =
                    entity_type.ok_or_else(|| de::Error::missing_field("entity_type"))?;
                let name = name.ok_or_else(|| de::Error::missing_field("name"))?;
                let qualified_name =
                    qualified_name.ok_or_else(|| de::Error::missing_field("qualified_name"))?;
                let containers =
                    containers.ok_or_else(|| de::Error::missing_field("containers"))?;
                let properties =
                    properties.ok_or_else(|| de::Error::missing_field("properties"))?;
                Ok(Entity::<Prop> {
                    id,
                    entity_type,
                    name,
                    qualified_name,
                    containers,
                    properties,
                })
            }
        }

        const FIELDS: &'static [&'static str] = &[
            "id",
            "entity_type",
            "name",
            "qualified_name",
            "containers",
            "properties",
        ];
        deserializer.deserialize_struct("Entity", FIELDS, EntityVisitor::<Prop> { _t: PhantomData })
    }
}

pub trait EntityPropMutator
where
    Self: Clone + Debug + PartialEq + Eq + crate::fts::ToDocString,
{
    fn new_project(definition: &ProjectDef) -> Result<Self, RegistryError>;
    fn new_source(definition: &SourceDef) -> Result<Self, RegistryError>;
    fn new_anchor(definition: &AnchorDef) -> Result<Self, RegistryError>;
    fn new_anchor_feature(definition: &AnchorFeatureDef) -> Result<Self, RegistryError>;
    fn new_derived_feature(definition: &DerivedFeatureDef)
        -> Result<Self, RegistryError>;

    /**
     * Function will be called when 2 entities are connected.
     * EntityProp may need to update internal state accordingly.
     */
    fn connect(
        from: &mut Entity<Self>,
        from_id: Uuid,
        to: &mut Entity<Self>,
        to_id: Uuid,
        edge_type: EdgeType,
    );

    /**
     * Function will be called when 2 entities are disconnected.
     * EntityProp may need to update internal state accordingly.
     */
    fn disconnect(
        from: &mut Entity<Self>,
        from_id: Uuid,
        to: &mut Entity<Self>,
        to_id: Uuid,
        edge_type: EdgeType,
    );
}

