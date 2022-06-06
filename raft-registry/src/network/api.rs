use log::{debug, trace};
use poem::web::Data;
use poem_openapi::{
    param::{Header, Path, Query},
    payload::Json,
    OpenApi, Tags,
};
use registry_api::{
    AnchorDef, AnchorFeatureDef, CreationResponse, DerivedFeatureDef, Entities, Entity,
    EntityLineage, FeathrApiProvider, FeathrApiRequest, FeathrApiResponse, ProjectDef, SourceDef,
};

use crate::RaftRegistryApp;

#[derive(Tags)]
enum ApiTags {
    Project,
    DataSource,
    Anchor,
    AnchorFeature,
    DerivedFeature,
    Feature,
}

pub struct FeathrApi;

#[OpenApi]
impl FeathrApi {
    async fn request(
        &self,
        data: Data<&RaftRegistryApp>,
        opt_seq: Option<u64>,
        req: FeathrApiRequest,
    ) -> FeathrApiResponse {
        let forward = match data.0.raft.is_leader().await {
            Ok(_) => {
                // This instance is the leader, do not forward
                trace!("This node is the leader");
                false
            }
            Err(e) => {
                trace!("Check leader failed, error is {:?}", e);
                match opt_seq {
                    Some(seq) => match data.0.store.state_machine.read().await.last_applied_log {
                        Some(l) => {
                            // Check is local log index is newer than required seq, forward if local is out dated
                            trace!("Local log index is {}, required seq is {}", l.index, seq);
                            l.index < seq
                        }
                        None => {
                            // There is no local log index, so we have to forward
                            trace!("No last applied log");
                            true
                        }
                    },
                    // opt_seq is not set, forward to the leader for consistent read
                    None => true,
                }
            }
        };
        if forward {
            debug!("The request is being forwarded to the leader");
        } else {
            debug!("The request is being handled locally");
        }

        // TODO:  Forward to the leader when `forward` is `true`

        data.0
            .store
            .state_machine
            .write()
            .await
            .registry
            .request(req)
            .await
    }

    #[oai(path = "/projects", method = "get", tag = "ApiTags::Project")]
    async fn get_projects(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        self.request(
            data,
            opt_seq.0,
            FeathrApiRequest::GetProjects {
                keyword: keyword.0,
                size: size.0,
                offset: offset.0,
            },
        )
        .await
        .into_entities()
        .map(Json)
    }

    #[oai(path = "/projects", method = "post", tag = "ApiTags::Project")]
    async fn new_project(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        def: Json<ProjectDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        self.request(
            data,
            opt_seq.0,
            FeathrApiRequest::CreateProject { definition: def.0 },
        )
        .await
        .into_uuid()
        .map(|v| Json(v.into()))
    }

    #[oai(path = "/projects/:project", method = "get", tag = "ApiTags::Project")]
    async fn get_project(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        self.request(
            data,
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
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
    ) -> poem::Result<Json<EntityLineage>> {
        self.request(
            data,
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
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        self.request(
            data,
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
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        self.request(
            data,
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
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        def: Json<SourceDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        self.request(
            data,
            opt_seq.0,
            FeathrApiRequest::CreateProjectDataSource {
                project_id_or_name: project.0,
                definition: def.0,
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
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        source: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        self.request(
            data,
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
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        self.request(
            data,
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
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        def: Json<DerivedFeatureDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        self.request(
            data,
            opt_seq.0,
            FeathrApiRequest::CreateProjectDerivedFeature {
                project_id_or_name: project.0,
                definition: def.0,
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
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        self.request(
            data,
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
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        def: Json<AnchorDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        self.request(
            data,
            opt_seq.0,
            FeathrApiRequest::CreateProjectAnchor {
                project_id_or_name: project.0,
                definition: def.0,
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
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        anchor: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        self.request(
            data,
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
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        anchor: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        self.request(
            data,
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
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        anchor: Path<String>,
        def: Json<AnchorFeatureDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        self.request(
            data,
            opt_seq.0,
            FeathrApiRequest::CreateAnchorFeature {
                project_id_or_name: project.0,
                anchor_id_or_name: anchor.0,
                definition: def.0,
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
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        feature: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        self.request(
            data,
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
        #[oai(name = "opt-seq")] opt_seq: Header<Option<u64>>,
        feature: Path<String>,
    ) -> poem::Result<Json<EntityLineage>> {
        self.request(
            data,
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
