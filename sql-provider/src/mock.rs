use std::{collections::HashMap, fs::File};

use serde::Deserialize;
use uuid::Uuid;

use registry_provider::*;

use crate::Registry;


#[cfg(any(mock, test))]
pub async fn load() -> crate::Registry<EntityProperty, EdgeProperty> {

    #[derive(Debug, Deserialize)]
    struct SampleData {
        #[serde(rename = "guidEntityMap")]
        guid_entity_map: HashMap<Uuid, EntityProperty>,
        #[serde(rename = "relations")]
        relations: Vec<EdgeProperty>,
    }
    let f = File::open("test-data/sample.json").unwrap();
    let data: SampleData = serde_json::from_reader(f).unwrap();
    let mut r = Registry::<EntityProperty, EdgeProperty>::load(
        data.guid_entity_map.into_iter().map(|(_, i)| i.into()),
        data.relations.into_iter().map(|i| i.into()),
    )
    .await
    .unwrap();
    let project = r.get_projects()[0].id;
    let subs: Vec<Uuid> = r
        .get_entities(|w| {
            w.entity_type == EntityType::AnchorFeature
                || w.entity_type == EntityType::DerivedFeature
                || w.entity_type == EntityType::Anchor
                || w.entity_type == EntityType::Source
        })
        .into_iter()
        .map(|e| e.id)
        .collect();
    for sub in subs {
        r.connect(
            sub,
            project,
            EdgeType::BelongsTo,
            EdgeProperty {
                edge_type: EdgeType::BelongsTo,
                from: sub,
                to: project,
            },
        )
        .unwrap();
    }
    r
}
