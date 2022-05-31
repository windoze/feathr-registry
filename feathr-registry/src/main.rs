use std::collections::{HashMap, HashSet};

use common_utils::{set, Logged};
use log::debug;
use poem::{
    error::NotFoundError,
    get, handler,
    listener::TcpListener,
    middleware::{Cors, Tracing},
    web::{Data, Json, Path, Query},
    EndpointExt, Route, Server,
};
use poem_openapi::param;
use registry_provider::{
    AnchorDef, AnchorFeatureDef, DerivedFeatureDef, EdgePropMutator, EdgeProperty, EdgeType,
    Entity, EntityProperty, EntityType, ProjectDef, RegistryProvider, SourceDef,
};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

mod api_models;
mod error;
mod spa_endpoint;

use error::IntoApiResult;

use crate::api_models::*;

#[cfg(feature = "sqlbackend")]
mod backend {
    use std::sync::Arc;

    use registry_provider::{EdgeProperty, EntityProperty, RegistryProvider};
    use tokio::sync::RwLock;
    use uuid::Uuid;

    use crate::error::IntoApiResult;

    #[derive(Clone)]
    pub struct RegistryData {
        pub registry: Arc<RwLock<sql_provider::Registry<EntityProperty, EdgeProperty>>>,
    }

    impl RegistryData {
        pub async fn new() -> Result<Self, anyhow::Error> {
            Ok(Self {
                registry: Arc::new(RwLock::new(sql_provider::load_registry().await?)),
            })
        }

        pub async fn get_id(&self, id_or_name: String) -> poem::Result<Uuid> {
            match Uuid::parse_str(&id_or_name) {
                Ok(id) => Ok(id),
                Err(_) => {
                    let id = self
                        .registry
                        .read()
                        .await
                        .get_entity_id(&id_or_name)
                        .await
                        .map_api_error()?;
                    Ok(id)
                }
            }
        }
    }
}

async fn search_entities(
    data: Data<&backend::RegistryData>,
    param: SearchParams,
    types: HashSet<EntityType>,
    scope: Option<Uuid>,
) -> poem::Result<Vec<Entity<EntityProperty>>> {
    Ok(data
        .registry
        .read()
        .await
        .search_entity(
            &param.keyword,
            types,
            scope,
            param.size.unwrap_or(100),
            param.offset.unwrap_or(0),
        )
        .await
        .map_api_error()?
        .into_iter()
        .collect())
}

#[handler]
async fn get_projects(
    data: Data<&backend::RegistryData>,
    Query(param): Query<SearchParams>,
) -> poem::Result<Json<EntitiesResponse>> {
    Ok(Json(if param.is_empty() {
        data.registry
            .read()
            .await
            .get_entry_points()
            .await
            .map_api_error()?
            .into_iter()
            .collect()
    } else {
        search_entities(data, param, set![EntityType::Project], None)
            .await?
            .into_iter()
            .collect()
    }))
}

#[handler]
async fn get_datasources(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
    Query(param): Query<SearchParams>,
) -> poem::Result<Json<EntitiesResponse>> {
    let id = data.get_id(project).await?;
    Ok(Json(
        data.registry
            .read()
            .await
            .get_project_children(id, set![EntityType::Source])
            .await
            .map_api_error()?
            .into_iter()
            .map(|w| w.into())
            .collect(),
    ))
}

#[handler]
async fn get_project_features(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
    Query(param): Query<SearchParams>,
) -> poem::Result<Json<EntitiesResponse>> {
    debug!("Get features for project {}", project);

    let project_id = data.get_id(project).await?;

    let feature_entities: Vec<Entity<EntityProperty>> = data
        .registry
        .read()
        .await
        .get_project_children(
            project_id,
            set![EntityType::AnchorFeature, EntityType::DerivedFeature],
        )
        .await
        .map_api_error()?;

    let feature_ids: HashSet<Uuid> = if param.keyword.is_empty() {
        // All features under the project
        feature_entities.iter().map(|e| e.id).collect()
    } else {
        // Features also exist in the search result
        data.registry
            .read()
            .await
            .search_entity(
                &param.keyword,
                set![EntityType::AnchorFeature, EntityType::DerivedFeature],
                Some(project_id),
                param.size.unwrap_or(100),
                param.offset.unwrap_or(0),
            )
            .await
            .map_api_error()?
            .into_iter()
            .map(|e| e.id)
            .collect()
    };

    let entities = feature_entities
        .into_iter()
        .filter(|e| feature_ids.contains(&e.id))
        .map(|e| e.into())
        .collect();
    Ok(Json(EntitiesResponse { entities }))
}

#[handler]
async fn get_project_anchors(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
    Query(param): Query<SearchParams>,
) -> poem::Result<Json<Value>> {
    todo!()
}

#[handler]
async fn get_project_derived_features(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
    Query(param): Query<SearchParams>,
) -> poem::Result<Json<Value>> {
    todo!()
}

#[handler]
async fn get_anchor_features(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
    Path(anchor): Path<String>,
    Query(param): Query<SearchParams>,
) -> poem::Result<Json<Value>> {
    todo!()
}

#[handler]
async fn get_feature(
    data: Data<&backend::RegistryData>,
    Path(feature): Path<String>,
) -> poem::Result<Json<Value>> {
    #[derive(Serialize)]
    struct Response {
        #[serde(rename = "referredEntities")]
        referred_entities: HashMap<String, String>,
        entity: EntityProperty,
    }
    let r = data
        .registry
        .read()
        .await
        .get_entity_by_qualified_name(&feature)
        .await
        .ok_or_else(|| NotFoundError)?;
    let r = serde_json::to_value(Response {
        referred_entities: Default::default(),
        entity: r.properties,
    })
    .unwrap();
    Ok(Json(r))
}

#[handler]
async fn get_feature_lineage(
    data: Data<&backend::RegistryData>,
    Path(feature): Path<String>,
) -> poem::Result<Json<FeatureLineageResponse>> {
    debug!("Feature name: {}", feature);
    let id = data.get_id(feature).await?;
    let (up_entities, up_edges) = data
        .registry
        .read()
        .await
        .bfs(id, EdgeType::Consumes, 100)
        .await
        .map_api_error()?;
    let (down_entities, down_edges) = data
        .registry
        .read()
        .await
        .bfs(id, EdgeType::Produces, 100)
        .await
        .map_api_error()?;
    let mut entities: HashMap<String, EntityResponse> = HashMap::new();
    for e in up_entities.into_iter().chain(down_entities.into_iter()) {
        entities.insert(e.id.to_string(), e.properties.into());
    }
    let mut edges: HashSet<EdgeResponse> = HashSet::new();
    for e in up_edges.into_iter().chain(down_edges.into_iter()) {
        edges.insert(e.properties.reflection().into());
    }
    Ok(Json(FeatureLineageResponse {
        guid_entity_map: entities,
        relations: edges.into_iter().map(|e| e.into()).collect(),
    }))
}

#[handler]
async fn get_project(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
) -> poem::Result<Json<FeatureLineageResponse>> {
    debug!("Project name: {}", project);

    let (entities, edges) = data
        .registry
        .read()
        .await
        .get_project(&project)
        .await
        .map_api_error()?;

    let entities: HashMap<String, EntityResponse> = entities
        .into_iter()
        .map(|e| (e.id.to_string(), e.properties.into()))
        .collect();
    Ok(Json(FeatureLineageResponse {
        guid_entity_map: entities,
        relations: edges.into_iter().map(|e| e.properties.into()).collect(),
    }))
}

#[handler]
async fn new_project(
    data: Data<&backend::RegistryData>,
    Json(def): Json<ProjectDef>,
) -> poem::Result<Json<CreationResponse>> {
    let guid = data
        .registry
        .write()
        .await
        .new_project(&def)
        .await
        .map_api_error()?;
    Ok(Json(guid.into()))
}

#[handler]
async fn new_anchor(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
    Json(def): Json<AnchorDef>,
) -> poem::Result<Json<CreationResponse>> {
    let id = data.get_id(project).await?;
    let guid = data
        .registry
        .write()
        .await
        .new_anchor(id, &def)
        .await
        .map_api_error()?;
    Ok(Json(guid.into()))
}

#[handler]
async fn new_datasource(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
    Json(def): Json<SourceDef>,
) -> poem::Result<Json<CreationResponse>> {
    let id = data.get_id(project).await?;
    let guid = data
        .registry
        .write()
        .await
        .new_source(id, &def)
        .await
        .map_api_error()?;
    Ok(Json(guid.into()))
}

#[handler]
async fn new_anchor_feature(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
    Path(anchor): Path<String>,
    Json(def): Json<AnchorFeatureDef>,
) -> poem::Result<Json<CreationResponse>> {
    let project_id = data.get_id(project).await?;
    let anchor_id = data.get_id(anchor).await?;
    let guid = data
        .registry
        .write()
        .await
        .new_anchor_feature(project_id, anchor_id, &def)
        .await
        .map_api_error()?;
    Ok(Json(guid.into()))
}

#[handler]
async fn new_derived_feature(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
    Json(def): Json<DerivedFeatureDef>,
) -> poem::Result<Json<CreationResponse>> {
    let id = data.get_id(project).await?;
    let guid = data
        .registry
        .write()
        .await
        .new_derived_feature(id, &def)
        .await
        .map_api_error()?;
    Ok(Json(guid.into()))
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    common_utils::init_logger();

    let data = backend::RegistryData::new().await?;

    let api_base = format!("/{}", std::env::var("API_BASE").unwrap_or_default());

    let api = Route::new()
        .at("/projects", get(get_projects).post(new_project))
        .at("/projects/:project", get(get_project))
        .at("/projects/:project/features", get(get_project_features))
        .at(
            "/projects/:project/datasources",
            get(get_datasources).post(new_datasource),
        )
        .at(
            "/projects/:project/anchors",
            get(get_project_anchors).post(new_anchor),
        )
        .at(
            "/projects/:project/anchors/:anchor/features",
            get(get_anchor_features).post(new_anchor_feature),
        )
        .at(
            "/projects/:project/derivedfeatures",
            get(get_project_derived_features).post(new_derived_feature),
        )
        .at("/features/:feature", get(get_feature))
        .at("/features/:feature/lineage", get(get_feature_lineage))
        .data(data);

    let app = Route::new()
        .nest(&api_base, api)
        .nest(
            "/",
            spa_endpoint::SpaEndpoint::new("./static-files", "index.html"),
        )
        .with(Cors::new())
        .with(Tracing);
    Server::new(TcpListener::bind(
        std::env::var("LISTENING_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string()),
    ))
    .run(app)
    .await
    .log()?;

    Ok(())
}
