use std::collections::{HashMap, HashSet};

use common_utils::{set, Logged};
use log::debug;
use poem::{
    error::{InternalServerError, NotFoundError},
    get, handler,
    listener::TcpListener,
    middleware::{Cors, Tracing},
    post,
    web::{Data, Json, Path, Query},
    EndpointExt, Route, Server,
};
use registry_provider::{
    AnchorDef, AnchorFeatureDef, DerivedFeatureDef, EdgePropMutator, EdgeProperty, EdgeType,
    Entity, EntityProperty, EntityType, ProjectDef, RegistryProvider, SourceDef,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

mod spa_endpoint;

#[cfg(feature = "sqlbackend")]
mod backend {
    use std::sync::Arc;

    use registry_provider::{EdgeProperty, EntityProperty};
    use tokio::sync::RwLock;

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
    }
}

#[handler]
async fn get_projects(data: Data<&backend::RegistryData>) -> poem::Result<Json<Vec<String>>> {
    Ok(Json(
        data.registry
            .read()
            .await
            .get_entry_points()
            .await
            .map_err(InternalServerError)?
            .into_iter()
            .map(|w| w.name)
            .collect(),
    ))
}

#[handler]
async fn get_project_datasources(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
) -> poem::Result<Json<Vec<EntityProperty>>> {
    let id = data
        .registry
        .read()
        .await
        .get_entity_id(&project)
        .await
        .map_err(InternalServerError)?;
    Ok(Json(
        data.registry
            .read()
            .await
            .get_project_children(id, set![EntityType::Source])
            .await
            .map_err(InternalServerError)?
            .into_iter()
            .map(|w| w.properties)
            .collect(),
    ))
}

#[derive(Serialize)]
struct FeatureResponseItem {
    name: String,
    id: Uuid,
    #[serde(rename = "qualifiedName")]
    qualified_name: String,
}
#[derive(Serialize)]
struct FeaturesResponse {
    features: Vec<FeatureResponseItem>,
}

#[derive(Debug, Deserialize)]
struct SearchFeatureParams {
    keyword: String,
    size: Option<usize>,
    offset: Option<usize>,
}

#[handler]
async fn get_project_features(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
    Query(param): Query<SearchFeatureParams>,
) -> poem::Result<Json<FeaturesResponse>> {
    debug!("Get features for project {}", project);

    let project_id = data
        .registry
        .read()
        .await
        .get_entity_id(&project)
        .await
        .map_err(|_| NotFoundError)?;

    let feature_entities: Vec<Entity<EntityProperty>> = data
        .registry
        .read()
        .await
        .get_project_children(
            project_id,
            set![EntityType::AnchorFeature, EntityType::DerivedFeature],
        )
        .await
        .map_err(InternalServerError)?;

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
            .map_err(InternalServerError)?
            .into_iter()
            .map(|e| e.id)
            .collect()
    };

    let features = feature_entities
        .into_iter()
        .filter(|e| feature_ids.contains(&e.id))
        .map(|e| FeatureResponseItem {
            name: e.name,
            id: e.id,
            qualified_name: e.qualified_name,
        })
        .collect();

    Ok(Json(FeaturesResponse { features }))
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

#[derive(Serialize)]
struct FeatureLineageResponse {
    #[serde(rename = "guidEntityMap")]
    guid_entity_map: HashMap<Uuid, EntityProperty>,
    relations: Vec<EdgeProperty>,
}

#[handler]
async fn get_feature_lineage(
    data: Data<&backend::RegistryData>,
    Path(feature): Path<String>,
) -> poem::Result<Json<FeatureLineageResponse>> {
    debug!("Feature name: {}", feature);
    let id = data
        .registry
        .read()
        .await
        .get_entity_id(&feature)
        .await
        .map_err(|_| NotFoundError)?;
    let (up_entities, up_edges) = data
        .registry
        .read()
        .await
        .bfs(id, EdgeType::Consumes, 100)
        .await
        .map_err(InternalServerError)?;
    let (down_entities, down_edges) = data
        .registry
        .read()
        .await
        .bfs(id, EdgeType::Produces, 100)
        .await
        .map_err(InternalServerError)?;
    let mut entities: HashMap<Uuid, EntityProperty> = HashMap::new();
    for e in up_entities.into_iter().chain(down_entities.into_iter()) {
        entities.insert(e.id, e.properties);
    }
    let mut edges: HashSet<EdgeProperty> = HashSet::new();
    for e in up_edges.into_iter().chain(down_edges.into_iter()) {
        edges.insert(e.properties.reflection());
    }
    Ok(Json(FeatureLineageResponse {
        guid_entity_map: entities,
        relations: edges.into_iter().collect(),
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
        .map_err(InternalServerError)?;

    let entities: HashMap<Uuid, EntityProperty> =
        entities.into_iter().map(|e| (e.id, e.properties)).collect();
    Ok(Json(FeatureLineageResponse {
        guid_entity_map: entities,
        relations: edges.into_iter().map(|e| e.properties).collect(),
    }))
}

#[derive(Copy, Clone, Debug, Serialize)]
struct CreationResponse {
    guid: Uuid,
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
        .map_err(InternalServerError)?;
    Ok(Json(CreationResponse { guid }))
}

#[handler]
async fn new_anchor(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
    Json(def): Json<AnchorDef>,
) -> poem::Result<Json<CreationResponse>> {
    let id = data
        .registry
        .read()
        .await
        .get_entity_id(&project)
        .await
        .map_err(|_| NotFoundError)?;
    let guid = data
        .registry
        .write()
        .await
        .new_anchor(id, &def)
        .await
        .map_err(InternalServerError)?;
    Ok(Json(CreationResponse { guid }))
}

#[handler]
async fn new_source(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
    Json(def): Json<SourceDef>,
) -> poem::Result<Json<CreationResponse>> {
    let id = data
        .registry
        .read()
        .await
        .get_entity_id(&project)
        .await
        .map_err(|_| NotFoundError)?;
    let guid = data
        .registry
        .write()
        .await
        .new_source(id, &def)
        .await
        .map_err(InternalServerError)?;
    Ok(Json(CreationResponse { guid }))
}

#[handler]
async fn new_anchor_feature(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
    Path(anchor): Path<String>,
    Json(def): Json<AnchorFeatureDef>,
) -> poem::Result<Json<CreationResponse>> {
    let project_id = data
        .registry
        .read()
        .await
        .get_entity_id(&project)
        .await
        .map_err(|_| NotFoundError)?;
    let anchor_id = data
        .registry
        .read()
        .await
        .get_entity_id(&anchor)
        .await
        .map_err(|_| NotFoundError)?;
    let guid = data
        .registry
        .write()
        .await
        .new_anchor_feature(project_id, anchor_id, &def)
        .await
        .map_err(InternalServerError)?;
    Ok(Json(CreationResponse { guid }))
}

#[handler]
async fn new_derived_feature(
    data: Data<&backend::RegistryData>,
    Path(project): Path<String>,
    Json(def): Json<DerivedFeatureDef>,
) -> poem::Result<Json<CreationResponse>> {
    let id = data
        .registry
        .read()
        .await
        .get_entity_id(&project)
        .await
        .map_err(|_| NotFoundError)?;
    let guid = data
        .registry
        .write()
        .await
        .new_derived_feature(id, &def)
        .await
        .map_err(InternalServerError)?;
    Ok(Json(CreationResponse { guid }))
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    common_utils::init_logger();

    let data = backend::RegistryData::new().await?;

    let api_base = format!("/{}", std::env::var("API_BASE").unwrap_or_default());

    let api = Route::new()
        .at(
            "/projects/:project/datasources",
            get(get_project_datasources),
        )
        .at("/projects", get(get_projects))
        .at("/projects/:project", get(get_project))
        .at("/projects/:project/features", get(get_project_features))
        .at("/features/:feature", get(get_feature))
        .at("/features/lineage/:feature", get(get_feature_lineage))
        .at("/projects", post(new_project))
        .at("/projects/:project/datasources", post(new_source))
        .at("/projects/:project/anchors", post(new_anchor))
        .at(
            "/projects/:project/anchors/:anchor/features",
            post(new_anchor_feature),
        )
        .at(
            "/projects/:project/derivedfeatures",
            post(new_derived_feature),
        )
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
