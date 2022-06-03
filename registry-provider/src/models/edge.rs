use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::EntityType;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EdgeType {
    // Feature/Source/AnchorGroup belongs to project
    BelongsTo,
    // Project Contains Feature/Source/AnchorGroup, AnchorGroup contains AnchorFeatures
    Contains,

    // AnchorGroup uses Source, DerivedFeature used Anchor/DerivedFeatures
    Consumes,
    // Source used by AnchorGroup, Anchor/DerivedFeatures derives DerivedFeature
    Produces,
}

impl Default for EdgeType {
    fn default() -> Self {
        EdgeType::BelongsTo // Whatever
    }
}

impl EdgeType {
    pub fn reflection(self) -> Self {
        match self {
            EdgeType::BelongsTo => EdgeType::Contains,
            EdgeType::Contains => EdgeType::BelongsTo,
            EdgeType::Consumes => EdgeType::Produces,
            EdgeType::Produces => EdgeType::Consumes,
        }
    }

    pub fn is_downstream(self) -> bool {
        matches!(self, EdgeType::Contains | EdgeType::Produces)
    }

    pub fn is_upstream(self) -> bool {
        matches!(self, EdgeType::BelongsTo | EdgeType::Consumes)
    }

    pub fn validate(&self, from: EntityType, to: EntityType) -> bool {
        match (from, to, self) {
            (EntityType::Project, EntityType::Source, EdgeType::Contains)
            | (EntityType::Project, EntityType::Anchor, EdgeType::Contains)
            | (EntityType::Project, EntityType::AnchorFeature, EdgeType::Contains)
            | (EntityType::Project, EntityType::DerivedFeature, EdgeType::Contains)
            | (EntityType::Source, EntityType::Project, EdgeType::BelongsTo)
            | (EntityType::Source, EntityType::Anchor, EdgeType::Produces)
            | (EntityType::Source, EntityType::AnchorFeature, EdgeType::Produces)
            | (EntityType::Anchor, EntityType::Project, EdgeType::BelongsTo)
            | (EntityType::Anchor, EntityType::Source, EdgeType::Consumes)
            | (EntityType::Anchor, EntityType::AnchorFeature, EdgeType::Contains)
            | (EntityType::AnchorFeature, EntityType::Project, EdgeType::BelongsTo)
            | (EntityType::AnchorFeature, EntityType::Source, EdgeType::Consumes)
            | (EntityType::AnchorFeature, EntityType::Anchor, EdgeType::BelongsTo)
            | (EntityType::AnchorFeature, EntityType::DerivedFeature, EdgeType::Produces)
            | (EntityType::DerivedFeature, EntityType::Project, EdgeType::BelongsTo)
            | (EntityType::DerivedFeature, EntityType::AnchorFeature, EdgeType::Consumes)
            | (EntityType::DerivedFeature, EntityType::DerivedFeature, EdgeType::Produces) => true,
            (EntityType::DerivedFeature, EntityType::DerivedFeature, EdgeType::Consumes) => true,

            _ => return false,
        }
    }
}

#[derive(Clone, Debug, Eq)]
pub struct Edge<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    pub from: Uuid,
    pub to: Uuid,
    pub edge_type: EdgeType,
    pub properties: Prop,
}

impl<Prop> Edge<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq + EdgePropMutator,
{
    pub fn reflection(&self) -> Self {
        Self {
            from: self.to,
            to: self.from,
            edge_type: self.edge_type.reflection(),
            properties: self.properties.reflection(),
        }
    }
}

impl<Prop> PartialEq for Edge<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    fn eq(&self, other: &Self) -> bool {
        self.from == other.from && self.to == other.to && self.edge_type == other.edge_type
    }
}

impl<Prop> Hash for Edge<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.from.hash(state);
        self.to.hash(state);
        self.edge_type.hash(state);
    }
}

impl<Prop> Serialize for Edge<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq + Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut entity = serializer.serialize_struct("Edge", 4)?;
        entity.serialize_field("from", &self.from)?;
        entity.serialize_field("to", &self.to)?;
        entity.serialize_field("edge_type", &self.edge_type)?;
        entity.serialize_field("properties", &self.properties)?;
        entity.end()
    }
}

impl<'de, Prop> Deserialize<'de> for Edge<Prop>
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
            From,
            To,
            EdgeType,
            Properties,
        }
        struct EdgeVisitor<T> {
            _t: std::marker::PhantomData<T>,
        }

        impl<'de, Prop> Visitor<'de> for EdgeVisitor<Prop>
        where
            Prop: Clone + Debug + PartialEq + Eq + Deserialize<'de>,
        {
            type Value = Edge<Prop>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct Edge")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Edge<Prop>, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let from = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let to = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let edge_type = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let properties = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(3, &self))?;
                Ok(Edge::<Prop> {
                    from,
                    to,
                    edge_type,
                    properties,
                })
            }

            fn visit_map<V>(self, mut map: V) -> Result<Edge<Prop>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut from = None;
                let mut to = None;
                let mut edge_type = None;
                let mut properties = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::From => {
                            if from.is_some() {
                                return Err(de::Error::duplicate_field("from"));
                            }
                            from = Some(map.next_value()?);
                        }
                        Field::To => {
                            if to.is_some() {
                                return Err(de::Error::duplicate_field("to"));
                            }
                            to = Some(map.next_value()?);
                        }
                        Field::EdgeType => {
                            if edge_type.is_some() {
                                return Err(de::Error::duplicate_field("edge_type"));
                            }
                            edge_type = Some(map.next_value()?);
                        }
                        Field::Properties => {
                            if properties.is_some() {
                                return Err(de::Error::duplicate_field("properties"));
                            }
                            properties = Some(map.next_value()?);
                        }
                    }
                }
                let from = from.ok_or_else(|| de::Error::missing_field("from"))?;
                let to = to.ok_or_else(|| de::Error::missing_field("to"))?;
                let edge_type = edge_type.ok_or_else(|| de::Error::missing_field("edge_type"))?;
                let properties =
                    properties.ok_or_else(|| de::Error::missing_field("properties"))?;
                Ok(Edge::<Prop> {
                    from,
                    to,
                    edge_type,
                    properties,
                })
            }
        }

        const FIELDS: &'static [&'static str] = &["from", "to", "edge_type", "properties"];
        deserializer.deserialize_struct("Edge", FIELDS, EdgeVisitor::<Prop> { _t: PhantomData })
    }
}

pub trait EdgePropMutator
where
    Self: Clone + Debug + PartialEq + Eq,
{
    fn new(from_id: Uuid, to_id: Uuid, edge_type: EdgeType) -> Self;

    /**
     * Get the refection of this edge, e.g. (A contains B) -> (B belongsTo A)
     */
    fn reflection(&self) -> Self;
}
