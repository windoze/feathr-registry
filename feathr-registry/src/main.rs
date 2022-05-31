use std::collections::{HashMap, HashSet};

use common_utils::{set, Logged};
use log::debug;
use poem::{
    error::NotFoundError,
    listener::TcpListener,
    middleware::{Cors, Tracing},
    web::Data,
    EndpointExt, Route, Server,
};
use poem_openapi::{
    param::{Path, Query},
    payload::Json,
    OpenApi, OpenApiService,
};
use registry_provider::{
    EdgePropMutator, EdgeType, Entity, EntityProperty, EntityType, RegistryProvider,
};
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

struct FeathrApi;

#[OpenApi]
impl FeathrApi {
    #[oai(path = "/projects", method = "get")]
    async fn get_projects(
        &self,
        data: Data<&backend::RegistryData>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        Ok(Json(if keyword.0.is_none() {
            data.registry
                .read()
                .await
                .get_entry_points()
                .await
                .map_api_error()?
                .into_iter()
                .collect()
        } else {
            search_entities(
                data,
                keyword.0,
                size.0,
                offset.0,
                set![EntityType::Project],
                None,
            )
            .await?
            .into_iter()
            .collect()
        }))
    }

    #[oai(path = "/projects", method = "post")]
    async fn new_project(
        &self,
        data: Data<&backend::RegistryData>,
        def: Json<ProjectDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        let guid = data
            .registry
            .write()
            .await
            .new_project(&def.0.try_into()?)
            .await
            .map_api_error()?;
        Ok(Json(guid.into()))
    }

    #[oai(path = "/projects/:project", method = "get")]
    async fn get_project(
        &self,
        data: Data<&backend::RegistryData>,
        project: Path<String>,
    ) -> poem::Result<Json<EntityLineage>> {
        debug!("Project name: {}", project.0);

        let (entities, edges) = data
            .registry
            .read()
            .await
            .get_project(&project.0)
            .await
            .map_api_error()?;

        let entities: HashMap<String, api_models::Entity> = entities
            .into_iter()
            .map(|e| (e.id.to_string(), e.properties.into()))
            .collect();
        Ok(Json(EntityLineage {
            guid_entity_map: entities,
            relations: edges.into_iter().map(|e| e.properties.into()).collect(),
        }))
    }

    #[oai(path = "/projects/:project/features", method = "get")]
    async fn get_project_features(
        &self,
        data: Data<&backend::RegistryData>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        debug!("Get features for project {}", project.0);

        let project_id = data.get_id(project.0).await?;

        let feature_entities: Vec<Entity<EntityProperty>> = data
            .registry
            .read()
            .await
            .get_children(
                project_id,
                set![EntityType::AnchorFeature, EntityType::DerivedFeature],
            )
            .await
            .map_api_error()?;

        let feature_ids: HashSet<Uuid> = if keyword.0.is_none() {
            // All features under the project
            feature_entities.iter().map(|e| e.id).collect()
        } else {
            // Features also exist in the search result
            search_entities(
                data,
                keyword.0,
                size.0,
                offset.0,
                set![EntityType::AnchorFeature, EntityType::DerivedFeature],
                Some(project_id),
            )
            .await?
            .into_iter()
            .map(|e| e.id)
            .collect()
        };

        let entities = feature_entities
            .into_iter()
            .filter(|e| feature_ids.contains(&e.id))
            .map(|e| e.into())
            .collect();
        Ok(Json(Entities { entities }))
    }

    #[oai(path = "/projects/:project/datasources", method = "get")]
    async fn get_datasources(
        &self,
        data: Data<&backend::RegistryData>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        let id = data.get_id(project.0).await?;
        Ok(Json(if keyword.0.is_none() {
            data.registry
                .read()
                .await
                .get_children(id, set![EntityType::Source])
                .await
                .map_api_error()?
                .into_iter()
                .map(|w| w.into())
                .collect()
        } else {
            search_entities(
                data,
                keyword.0,
                size.0,
                offset.0,
                set![EntityType::Source],
                Some(id),
            )
            .await?
            .into_iter()
            .collect()
        }))
    }

    #[oai(path = "/projects/:project/datasources", method = "post")]
    async fn new_datasource(
        &self,
        data: Data<&backend::RegistryData>,
        project: Path<String>,
        def: Json<SourceDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        let id = data.get_id(project.0).await?;
        let guid = data
            .registry
            .write()
            .await
            .new_source(id, &def.0.try_into()?)
            .await
            .map_api_error()?;
        Ok(Json(guid.into()))
    }

    #[oai(path = "/projects/:project/derivedfeatures", method = "get")]
    async fn get_project_derived_features(
        &self,
        data: Data<&backend::RegistryData>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        let id = data.get_id(project.0).await?;
        Ok(Json(if keyword.0.is_none() {
            data.registry
                .read()
                .await
                .get_children(id, set![EntityType::DerivedFeature])
                .await
                .map_api_error()?
                .into_iter()
                .map(|w| w.into())
                .collect()
        } else {
            search_entities(
                data,
                keyword.0,
                size.0,
                offset.0,
                set![EntityType::DerivedFeature],
                Some(id),
            )
            .await?
            .into_iter()
            .collect()
        }))
    }

    #[oai(path = "/projects/:project/derivedfeatures", method = "post")]
    async fn new_derived_feature(
        &self,
        data: Data<&backend::RegistryData>,
        project: Path<String>,
        def: Json<DerivedFeatureDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        let id = data.get_id(project.0).await?;
        let guid = data
            .registry
            .write()
            .await
            .new_derived_feature(id, &def.0.try_into()?)
            .await
            .map_api_error()?;
        Ok(Json(guid.into()))
    }

    #[oai(path = "/projects/:project/anchors", method = "get")]
    async fn get_project_anchors(
        &self,
        data: Data<&backend::RegistryData>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        let id = data.get_id(project.0).await?;
        Ok(Json(if keyword.0.is_none() {
            data.registry
                .read()
                .await
                .get_children(id, set![EntityType::Anchor])
                .await
                .map_api_error()?
                .into_iter()
                .map(|w| w.into())
                .collect()
        } else {
            search_entities(
                data,
                keyword.0,
                size.0,
                offset.0,
                set![EntityType::Anchor],
                Some(id),
            )
            .await?
            .into_iter()
            .collect()
        }))
    }

    #[oai(path = "/projects/:project/anchors", method = "post")]
    async fn new_anchor(
        &self,
        data: Data<&backend::RegistryData>,
        project: Path<String>,
        def: Json<AnchorDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        let id = data.get_id(project.0).await?;
        let guid = data
            .registry
            .write()
            .await
            .new_anchor(id, &def.0.try_into()?)
            .await
            .map_api_error()?;
        Ok(Json(guid.into()))
    }

    #[allow(unused)]
    #[oai(path = "/projects/:project/anchors/:anchor/features", method = "get")]
    async fn get_anchor_features(
        &self,
        data: Data<&backend::RegistryData>,
        project: Path<String>,
        anchor: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        // TODO: Make sure the anchor is under the project
        let anchor_id = data.get_id(anchor.0).await?;
        Ok(Json(if keyword.0.is_none() {
            data.registry
                .read()
                .await
                .get_children(anchor_id, set![EntityType::AnchorFeature])
                .await
                .map_api_error()?
                .into_iter()
                .map(|w| w.into())
                .collect()
        } else {
            search_entities(
                data,
                keyword.0,
                size.0,
                offset.0,
                set![EntityType::AnchorFeature],
                Some(anchor_id),
            )
            .await?
            .into_iter()
            .collect()
        }))
    }

    #[oai(path = "/projects/:project/anchors/:anchor/features", method = "post")]
    async fn new_anchor_feature(
        &self,
        data: Data<&backend::RegistryData>,
        project: Path<String>,
        anchor: Path<String>,
        def: Json<AnchorFeatureDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        let project_id = data.get_id(project.0).await?;
        let anchor_id = data.get_id(anchor.0).await?;
        let guid = data
            .registry
            .write()
            .await
            .new_anchor_feature(project_id, anchor_id, &def.0.try_into()?)
            .await
            .map_api_error()?;
        Ok(Json(guid.into()))
    }

    #[oai(path = "/features/:feature", method = "get")]
    async fn get_feature(
        &self,
        data: Data<&backend::RegistryData>,
        feature: Path<String>,
    ) -> poem::Result<Json<api_models::Entity>> {
        let r = data
            .registry
            .read()
            .await
            .get_entity_by_qualified_name(&feature)
            .await
            .ok_or_else(|| NotFoundError)?;
        Ok(Json(r.properties.into()))
    }

    #[oai(path = "/features/:feature/lineage", method = "get")]
    async fn get_feature_lineage(
        &self,
        data: Data<&backend::RegistryData>,
        feature: Path<String>,
    ) -> poem::Result<Json<EntityLineage>> {
        debug!("Feature name: {}", feature.0);
        let id = data.get_id(feature.0).await?;
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
        let mut entities: HashMap<String, api_models::Entity> = HashMap::new();
        for e in up_entities.into_iter().chain(down_entities.into_iter()) {
            entities.insert(e.id.to_string(), e.properties.into());
        }
        let mut edges: HashSet<Relationship> = HashSet::new();
        for e in up_edges.into_iter().chain(down_edges.into_iter()) {
            edges.insert(e.properties.reflection().into());
        }
        Ok(Json(EntityLineage {
            guid_entity_map: entities,
            relations: edges.into_iter().map(|e| e.into()).collect(),
        }))
    }
}

async fn search_entities(
    data: Data<&backend::RegistryData>,
    keyword: Option<String>,
    size: Option<usize>,
    offset: Option<usize>,
    types: HashSet<EntityType>,
    scope: Option<Uuid>,
) -> poem::Result<Vec<Entity<EntityProperty>>> {
    Ok(data
        .registry
        .read()
        .await
        .search_entity(
            &keyword.unwrap_or_default(),
            types,
            scope,
            size.unwrap_or(100),
            offset.unwrap_or(0),
        )
        .await
        .map_api_error()?
        .into_iter()
        .collect())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    common_utils::init_logger();

    let api_base = format!("/{}", std::env::var("API_BASE").unwrap_or_default());
    let server_addr = format!(
        "{}{}",
        std::env::var("SERVER_ADDR").unwrap_or_else(|_| "http://localhost:3000".to_string()),
        api_base,
    );

    let data = backend::RegistryData::new().await?;

    let api_service = OpenApiService::new(FeathrApi, "TestApi", "1.0.0").server(&server_addr);
    let ui = api_service.swagger_ui();
    let spec = api_service.spec();
    let route = Route::new()
        .nest(&api_base, api_service)
        .nest("/docs", ui)
        .at("/spec", poem::endpoint::make_sync(move |_| spec.clone()))
        .nest(
            "/",
            spa_endpoint::SpaEndpoint::new("./static-files", "index.html"),
        )
        .with(Cors::new())
        .with(Tracing)
        .data(data);
    Server::new(TcpListener::bind(
        std::env::var("LISTENING_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".to_string()),
    ))
    .run(route)
    .await
    .log()?;

    Ok(())
}
