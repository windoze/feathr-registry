use registry_provider::{ToDocString, SerializableRegistry};
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    ser::SerializeStruct,
    Deserialize, Serialize,
};
use std::{fmt::Debug, marker::PhantomData};

use crate::Registry;

impl<EntityProp, EdgeProp> Serialize for Registry<EntityProp, EdgeProp>
where
    EntityProp: Clone + Debug + PartialEq + Eq + ToDocString + Serialize,
    EdgeProp: Clone + Debug + PartialEq + Eq + Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut entity = serializer.serialize_struct("Registry", 2)?;
        entity.serialize_field("graph", &self.graph)?;
        entity.serialize_field("deleted", &self.deleted)?;
        entity.end()
    }
}

impl<'de, EntityProp, EdgeProp> Deserialize<'de> for Registry<EntityProp, EdgeProp>
where
    EntityProp: Clone + Debug + PartialEq + Eq + ToDocString + Deserialize<'de>,
    EdgeProp: Clone + Debug + PartialEq + Eq + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            Graph,
            Deleted,
        }
        struct RegistryVisitor<EntityProp, EdgeProp> {
            _t1: std::marker::PhantomData<EntityProp>,
            _t2: std::marker::PhantomData<EdgeProp>,
        }

        impl<'de, EntityProp, EdgeProp> Visitor<'de> for RegistryVisitor<EntityProp, EdgeProp>
        where
            EntityProp: Clone + Debug + PartialEq + Eq + ToDocString + Deserialize<'de>,
            EdgeProp: Clone + Debug + PartialEq + Eq + Deserialize<'de>,
        {
            type Value = Registry<EntityProp, EdgeProp>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct Registry")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Registry<EntityProp, EdgeProp>, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let graph = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let deleted = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(Registry::<EntityProp, EdgeProp>::from_content(
                    graph, deleted,
                ))
            }

            fn visit_map<V>(self, mut map: V) -> Result<Registry<EntityProp, EdgeProp>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut graph = None;
                let mut deleted = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Graph => {
                            if graph.is_some() {
                                return Err(de::Error::duplicate_field("graph"));
                            }
                            graph = Some(map.next_value()?);
                        }
                        Field::Deleted => {
                            if deleted.is_some() {
                                return Err(de::Error::duplicate_field("deleted"));
                            }
                            deleted = Some(map.next_value()?);
                        }
                    }
                }
                let graph = graph.ok_or_else(|| de::Error::missing_field("graph"))?;
                let deleted = deleted.ok_or_else(|| de::Error::missing_field("deleted"))?;
                Ok(Registry::<EntityProp, EdgeProp>::from_content(
                    graph, deleted,
                ))
            }
        }

        const FIELDS: &'static [&'static str] = &["graph", "deleted"];
        deserializer.deserialize_struct(
            "Registry",
            FIELDS,
            RegistryVisitor::<EntityProp, EdgeProp> {
                _t1: PhantomData,
                _t2: PhantomData,
            },
        )
    }
}

impl<'de, EntityProp, EdgeProp> SerializableRegistry<'de> for Registry<EntityProp, EdgeProp>
where
    EntityProp: Clone + Debug + PartialEq + Eq + ToDocString + Serialize + Deserialize<'de>,
    EdgeProp: Clone + Debug + PartialEq + Eq + Serialize + Deserialize<'de>,
{
    fn take_snapshot(&self) -> Result<Vec<u8>, registry_provider::RegistryError> {
        // TODO: unwrap
        Ok(serde_json::to_vec(&self).unwrap())
    }

    fn load_snapshot(&mut self, data: &'de [u8]) -> Result<(), registry_provider::RegistryError> {
        // TODO: unwrap
        *self = serde_json::from_slice::<'de, Self>(&data).unwrap();
        Ok(())
    }
}