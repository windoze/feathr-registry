use std::sync::Arc;

use async_trait::async_trait;
use poem::{web::Data, Endpoint, IntoResponse, Middleware, Request, Response};
use poem_openapi::{
    param::{Header, Path, Query},
    payload::Json,
    OpenApi, Tags,
};
use registry_api::{
    AnchorDef, AnchorFeatureDef, CreationResponse, DerivedFeatureDef, Entities, Entity,
    EntityLineage, FeathrApiRequest, ProjectDef, SourceDef,
};
use uuid::Uuid;

use crate::{RaftRegistryApp, RegistryStore, OPT_SEQ_HEADER_NAME};

#[derive(Tags)]
enum ApiTags {
    Project,
    DataSource,
    Anchor,
    AnchorFeature,
    DerivedFeature,
    Feature,
}

pub struct RaftSequencer {
    store: Arc<RegistryStore>,
}

impl RaftSequencer {
    pub fn new(store: Arc<RegistryStore>) -> Self {
        Self { store }
    }
}

impl<E: Endpoint> Middleware<E> for RaftSequencer {
    type Output = RaftSequencerImpl<E>;

    fn transform(&self, ep: E) -> Self::Output {
        RaftSequencerImpl {
            ep,
            store: self.store.clone(),
        }
    }
}

pub struct RaftSequencerImpl<E> {
    ep: E,
    store: Arc<RegistryStore>,
}

#[async_trait]
impl<E: Endpoint> Endpoint for RaftSequencerImpl<E> {
    type Output = Response;

    async fn call(&self, req: Request) -> poem::Result<Self::Output> {
        let res = self.ep.call(req).await;
        let opt_seq = self
            .store
            .state_machine
            .read()
            .await
            .last_applied_log
            .map(|l| l.index);

        match res {
            Ok(resp) => {
                let resp = match opt_seq {
                    Some(v) => resp.with_header(OPT_SEQ_HEADER_NAME, v).into_response(),
                    None => resp.into_response(),
                };
                Ok(resp)
            }
            Err(err) => Err(err),
        }
    }
}

pub struct FeathrApi;

#[OpenApi]
impl FeathrApi {
    #[oai(path = "/projects", method = "get", tag = "ApiTags::Project")]
    async fn get_projects(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Vec<String>>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjects {
                    keyword: keyword.0,
                    size: size.0,
                    offset: offset.0,
                },
            )
            .await
            .into_entity_names()
            .map(Json)
    }

    #[oai(path = "/projects", method = "post", tag = "ApiTags::Project")]
    async fn new_project(
        &self,
        data: Data<&RaftRegistryApp>,
        def: Json<ProjectDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        data.0
            .request(
                None,
                FeathrApiRequest::CreateProject { definition },
            )
            .await
            .into_uuid()
            .map(|v| Json(v.into()))
    }

    #[oai(path = "/projects/:project", method = "get", tag = "ApiTags::Project")]
    async fn get_project(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProject {
                    id_or_name: project.0,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/lineage",
        method = "get",
        tag = "ApiTags::Project"
    )]
    async fn get_project_lineage(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
    ) -> poem::Result<Json<EntityLineage>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectLineage {
                    id_or_name: project.0,
                },
            )
            .await
            .into_lineage()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/features",
        method = "get",
        tag = "ApiTags::Project"
    )]
    async fn get_project_features(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectFeatures {
                    project_id_or_name: project.0,
                    keyword: keyword.0,
                    size: size.0,
                    offset: offset.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/datasources",
        method = "get",
        tag = "ApiTags::DataSource"
    )]
    async fn get_datasources(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDataSources {
                    project_id_or_name: project.0,
                    keyword: keyword.0,
                    size: size.0,
                    offset: offset.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/datasources",
        method = "post",
        tag = "ApiTags::DataSource"
    )]
    async fn new_datasource(
        &self,
        data: Data<&RaftRegistryApp>,
        project: Path<String>,
        def: Json<SourceDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        data.0
            .request(
                None,
                FeathrApiRequest::CreateProjectDataSource {
                    project_id_or_name: project.0,
                    definition,
                },
            )
            .await
            .into_uuid()
            .map(|v| Json(v.into()))
    }

    #[oai(
        path = "/projects/:project/datasources/:source",
        method = "get",
        tag = "ApiTags::DataSource"
    )]
    async fn get_datasource(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        source: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDataSource {
                    project_id_or_name: project.0,
                    id_or_name: source.0,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/derivedfeatures",
        method = "get",
        tag = "ApiTags::DerivedFeature"
    )]
    async fn get_project_derived_features(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDerivedFeatures {
                    project_id_or_name: project.0,
                    keyword: keyword.0,
                    size: size.0,
                    offset: offset.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/derivedfeatures",
        method = "post",
        tag = "ApiTags::DerivedFeature"
    )]
    async fn new_derived_feature(
        &self,
        data: Data<&RaftRegistryApp>,
        project: Path<String>,
        def: Json<DerivedFeatureDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        data.0
            .request(
                None,
                FeathrApiRequest::CreateProjectDerivedFeature {
                    project_id_or_name: project.0,
                    definition,
                },
            )
            .await
            .into_uuid()
            .map(|v| Json(v.into()))
    }

    #[oai(
        path = "/projects/:project/anchors",
        method = "get",
        tag = "ApiTags::Anchor"
    )]
    async fn get_project_anchors(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectAnchors {
                    project_id_or_name: project.0,
                    keyword: keyword.0,
                    size: size.0,
                    offset: offset.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/anchors",
        method = "post",
        tag = "ApiTags::Anchor"
    )]
    async fn new_anchor(
        &self,
        data: Data<&RaftRegistryApp>,
        project: Path<String>,
        def: Json<AnchorDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        data.0
            .request(
                None,
                FeathrApiRequest::CreateProjectAnchor {
                    project_id_or_name: project.0,
                    definition,
                },
            )
            .await
            .into_uuid()
            .map(|v| Json(v.into()))
    }

    #[oai(
        path = "/projects/:project/anchors/:anchor",
        method = "get",
        tag = "ApiTags::Anchor"
    )]
    async fn get_anchor(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        anchor: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectAnchor {
                    project_id_or_name: project.0,
                    id_or_name: anchor.0,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/anchors/:anchor/features",
        method = "get",
        tag = "ApiTags::AnchorFeature"
    )]
    async fn get_anchor_features(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        anchor: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetAnchorFeatures {
                    project_id_or_name: project.0,
                    anchor_id_or_name: anchor.0,
                    keyword: keyword.0,
                    size: size.0,
                    offset: offset.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/anchors/:anchor/features",
        method = "post",
        tag = "ApiTags::AnchorFeature"
    )]
    async fn new_anchor_feature(
        &self,
        data: Data<&RaftRegistryApp>,
        project: Path<String>,
        anchor: Path<String>,
        def: Json<AnchorFeatureDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        data.0
            .request(
                None,
                FeathrApiRequest::CreateAnchorFeature {
                    project_id_or_name: project.0,
                    anchor_id_or_name: anchor.0,
                    definition,
                },
            )
            .await
            .into_uuid()
            .map(|v| Json(v.into()))
    }

    #[oai(path = "/features/:feature", method = "get", tag = "ApiTags::Feature")]
    async fn get_feature(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        feature: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetFeature {
                    id_or_name: feature.0,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(
        path = "/features/:feature/lineage",
        method = "get",
        tag = "ApiTags::Feature"
    )]
    async fn get_feature_lineage(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        feature: Path<String>,
    ) -> poem::Result<Json<EntityLineage>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetFeatureLineage {
                    id_or_name: feature.0,
                },
            )
            .await
            .into_lineage()
            .map(Json)
    }
}
