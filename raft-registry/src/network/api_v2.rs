use common_utils::StringError;
use poem::{
    error::{BadRequest, InternalServerError},
    web::Data,
};
use poem_openapi::{
    param::{Header, Path, Query},
    payload::Json,
    OpenApi, Tags,
};
use registry_api::{
    AnchorDef, AnchorFeatureDef, ApiError, CreationResponse, DerivedFeatureDef, Entities, Entity,
    EntityLineage, FeathrApiRequest, ProjectDef, RbacResponse, SourceDef,
};
use registry_provider::{Credential, Permission};
use uuid::Uuid;

use crate::RaftRegistryApp;

#[derive(Tags)]
enum ApiTags {
    Project,
    DataSource,
    Anchor,
    AnchorFeature,
    DerivedFeature,
    Feature,
    Rbac,
}

pub struct FeathrApiV2;

#[OpenApi]
impl FeathrApiV2 {
    /// List or search names of all projects
    #[oai(
        path = "/projects",
        method = "get",
        tag = "ApiTags::Project",
        operation_id = "list_projects"
    )]
    async fn get_projects(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Search keywords
        keyword: Query<Option<String>>,
        /// Limit size of returned list
        size: Query<Option<usize>>,
        /// Starting offset of returned list
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Vec<String>>> {
        data.0
            .check_permission(credential.0, Some("global"), Permission::Read)
            .await?;
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

    /// Create new project
    #[oai(
        path = "/projects",
        method = "post",
        tag = "ApiTags::Project",
        operation_id = "new_project"
    )]
    async fn new_project(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        /// Creator of the project
        #[oai(name = "x-registry-requestor")]
        creator: Header<Option<String>>,
        /// Project definition
        def: Json<ProjectDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        data.0
            .check_permission(credential.0, Some("global"), Permission::Write)
            .await?;
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        if definition.created_by.is_empty() {
            definition.created_by = creator.0.unwrap_or_default();
        }
        let ret = data
            .0
            .request(None, FeathrApiRequest::CreateProject { definition })
            .await
            .into_uuid_and_version();
        // Grant project admin permission to the creator of the project.
        if let Ok((uuid, _)) = &ret {
            let ret = data
                .0
                .request(
                    None,
                    FeathrApiRequest::AddUserRole {
                        project_id_or_name: uuid.to_string(),
                        user: credential.0.clone(),
                        role: Permission::Admin,
                        requestor: credential.0.clone(),
                        reason: "Created project".to_string(),
                    },
                )
                .await;
            if let registry_api::FeathrApiResponse::Error(e) = ret {
                return Err(e.into())
            }
        }

        ret.map(|v| Json(v.into()))
    }

    /// Get project with specified name or id
    #[oai(
        path = "/projects/:project",
        method = "get",
        tag = "ApiTags::Project",
        operation_id = "get_project"
    )]
    async fn get_project(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
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

    /// Get project lineage
    #[oai(
        path = "/projects/:project/lineage",
        method = "get",
        tag = "ApiTags::Project",
        operation_id = "get_project_lineage"
    )]
    async fn get_project_lineage(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
    ) -> poem::Result<Json<EntityLineage>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
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

    /// Get or search features in the project
    #[oai(
        path = "/projects/:project/features",
        method = "get",
        tag = "ApiTags::Project",
        operation_id = "get_project_features"
    )]
    async fn get_project_features(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Search keywords
        keyword: Query<Option<String>>,
        /// Limit size of returned list
        size: Query<Option<usize>>,
        /// Starting offset of returned list
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
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

    /// Get or search data sources in the project
    #[oai(
        path = "/projects/:project/datasources",
        method = "get",
        tag = "ApiTags::DataSource"
    )]
    async fn get_datasources(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Search keywords
        keyword: Query<Option<String>>,
        /// Limit size of returned list
        size: Query<Option<usize>>,
        /// Starting offset of returned list
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
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

    /// Create a new data source in the project
    #[oai(
        path = "/projects/:project/datasources",
        method = "post",
        tag = "ApiTags::DataSource"
    )]
    async fn new_datasource(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
        /// Project name or id
        project: Path<String>,
        /// Data source definition
        def: Json<SourceDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Write)
            .await?;
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        if definition.created_by.is_empty() {
            definition.created_by = creator.0.unwrap_or_default();
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
            .into_uuid_and_version()
            .map(|v| Json(v.into()))
    }

    /// Get data source with specified name in a project
    #[oai(
        path = "/projects/:project/datasources/:source",
        method = "get",
        tag = "ApiTags::DataSource"
    )]
    async fn get_datasource(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Source name or id
        source: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
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

    /// Get all versions of a data source in a project
    #[oai(
        path = "/projects/:project/datasources/:source/versions",
        method = "get",
        tag = "ApiTags::DataSource"
    )]
    async fn get_datasource_versions(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Source name or id
        source: Path<String>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDataSourceVersions {
                    project_id_or_name: project.0,
                    id_or_name: source.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    /// Get specific version of a data source in a project
    #[oai(
        path = "/projects/:project/datasources/:source/versions/:version",
        method = "get",
        tag = "ApiTags::DataSource"
    )]
    async fn get_datasource_version(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Source name or id
        source: Path<String>,
        /// Version number
        version: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDataSourceVersion {
                    project_id_or_name: project.0,
                    id_or_name: source.0,
                    version: parse_version(version.0)?,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    /// Get or search derived features in a project
    #[oai(
        path = "/projects/:project/derivedfeatures",
        method = "get",
        tag = "ApiTags::DerivedFeature"
    )]
    async fn get_project_derived_features(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Search keywords
        keyword: Query<Option<String>>,
        /// Limit size of returned list
        size: Query<Option<usize>>,
        /// Starting offset of returned list
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
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

    /// Create a new derived feature in the project
    #[oai(
        path = "/projects/:project/derivedfeatures",
        method = "post",
        tag = "ApiTags::DerivedFeature"
    )]
    async fn new_derived_feature(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
        /// Project name or id
        project: Path<String>,
        /// Derived feature definition
        def: Json<DerivedFeatureDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Write)
            .await?;
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        if definition.created_by.is_empty() {
            definition.created_by = creator.0.unwrap_or_default();
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
            .into_uuid_and_version()
            .map(|v| Json(v.into()))
    }

    /// Get a derived feature in a project
    #[oai(
        path = "/projects/:project/derivedfeatures/:feature",
        method = "get",
        tag = "ApiTags::DerivedFeature"
    )]
    async fn get_project_derived_feature(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Feature name or id
        feature: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDerivedFeature {
                    project_id_or_name: project.0,
                    id_or_name: feature.0,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    /// Get all versions of the derived feature with specified name in a project
    #[oai(
        path = "/projects/:project/derivedfeatures/:feature/versions",
        method = "get",
        tag = "ApiTags::DerivedFeature"
    )]
    async fn get_project_derived_feature_versions(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Feature name or id
        feature: Path<String>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDerivedFeatureVersions {
                    project_id_or_name: project.0,
                    id_or_name: feature.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    /// Get specific version of a derived feature in a project
    #[oai(
        path = "/projects/:project/derivedfeatures/:feature/versions/:version",
        method = "get",
        tag = "ApiTags::DerivedFeature"
    )]
    async fn get_project_derived_feature_version(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Feature name or id
        feature: Path<String>,
        /// Version number
        version: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDerivedFeatureVersion {
                    project_id_or_name: project.0,
                    id_or_name: feature.0,
                    version: parse_version(version.0)?,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    /// Get or search anchors in a project
    #[oai(
        path = "/projects/:project/anchors",
        method = "get",
        tag = "ApiTags::Anchor"
    )]
    async fn get_project_anchors(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Search keywords
        keyword: Query<Option<String>>,
        /// Limit size of returned list
        size: Query<Option<usize>>,
        /// Starting offset of returned list
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
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

    /// Create a new anchor in the project
    #[oai(
        path = "/projects/:project/anchors",
        method = "post",
        tag = "ApiTags::Anchor"
    )]
    async fn new_anchor(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
        /// Project name or id
        project: Path<String>,
        /// Anchor definition
        def: Json<AnchorDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Write)
            .await?;
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        if definition.created_by.is_empty() {
            definition.created_by = creator.0.unwrap_or_default();
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
            .into_uuid_and_version()
            .map(|v| Json(v.into()))
    }

    /// Get an anchor in a project
    #[oai(
        path = "/projects/:project/anchors/:anchor",
        method = "get",
        tag = "ApiTags::Anchor"
    )]
    async fn get_anchor(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Anchor name or id
        anchor: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
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

    /// Get all versions of an anchor in a project
    #[oai(
        path = "/projects/:project/anchors/:anchor/versions",
        method = "get",
        tag = "ApiTags::Anchor"
    )]
    async fn get_anchor_versions(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Anchor name or id
        anchor: Path<String>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectAnchorVersions {
                    project_id_or_name: project.0,
                    id_or_name: anchor.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    /// Get specific version of an anchor in a project
    #[oai(
        path = "/projects/:project/anchors/:anchor/versions/:version",
        method = "get",
        tag = "ApiTags::Anchor"
    )]
    async fn get_anchor_version(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Anchor name or id
        anchor: Path<String>,
        /// Version number
        version: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectAnchorVersion {
                    project_id_or_name: project.0,
                    id_or_name: anchor.0,
                    version: parse_version(version.0)?,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    /// Get of search features in an anchor
    #[oai(
        path = "/projects/:project/anchors/:anchor/features",
        method = "get",
        tag = "ApiTags::AnchorFeature"
    )]
    async fn get_anchor_features(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Anchor name or id
        anchor: Path<String>,
        /// Search keywords
        keyword: Query<Option<String>>,
        /// Limit size of returned list
        size: Query<Option<usize>>,
        /// Starting offset of returned list
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
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

    /// Create a feature in an anchor
    #[oai(
        path = "/projects/:project/anchors/:anchor/features",
        method = "post",
        tag = "ApiTags::AnchorFeature"
    )]
    async fn new_anchor_feature(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
        /// Project name or id
        project: Path<String>,
        /// Anchor name or id
        anchor: Path<String>,
        /// Anchor feature definition
        def: Json<AnchorFeatureDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Write)
            .await?;
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        if definition.created_by.is_empty() {
            definition.created_by = creator.0.unwrap_or_default();
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
            .into_uuid_and_version()
            .map(|v| Json(v.into()))
    }

    /// Get a feature in an anchor
    #[oai(
        path = "/projects/:project/anchors/:anchor/features/:feature",
        method = "get",
        tag = "ApiTags::AnchorFeature"
    )]
    async fn get_project_anchor_feature(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Anchor name or id
        anchor: Path<String>,
        /// Feature name or id
        feature: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetAnchorFeature {
                    project_id_or_name: project.0,
                    anchor_id_or_name: anchor.0,
                    id_or_name: feature.0,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    /// Get all versions of a feature in an anchor
    #[oai(
        path = "/projects/:project/anchors/:anchor/features/:feature/versions",
        method = "get",
        tag = "ApiTags::AnchorFeature"
    )]
    async fn get_project_anchor_feature_versions(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Anchor name or id
        anchor: Path<String>,
        /// Feature name or id
        feature: Path<String>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetAnchorFeatureVersions {
                    project_id_or_name: project.0,
                    anchor_id_or_name: anchor.0,
                    id_or_name: feature.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    /// Get specific version of a feature in an anchor
    #[oai(
        path = "/projects/:project/anchors/:anchor/features/:feature/versions/:version",
        method = "get",
        tag = "ApiTags::AnchorFeature"
    )]
    async fn get_project_anchor_feature_version(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Project name or id
        project: Path<String>,
        /// Anchor name or id
        anchor: Path<String>,
        /// Feature name or id
        feature: Path<String>,
        /// Version number
        version: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetAnchorFeatureVersion {
                    project_id_or_name: project.0,
                    anchor_id_or_name: anchor.0,
                    id_or_name: feature.0,
                    version: parse_version(version.0)?,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    /// Get a feature
    #[oai(path = "/features/:feature", method = "get", tag = "ApiTags::Feature")]
    async fn get_feature(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Feature name or id
        feature: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&feature), Permission::Read)
            .await?;
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

    /// Get lineage of a feature
    #[oai(
        path = "/features/:feature/lineage",
        method = "get",
        tag = "ApiTags::Feature"
    )]
    async fn get_feature_lineage(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Feature name or id
        feature: Path<String>,
    ) -> poem::Result<Json<EntityLineage>> {
        data.0
            .check_permission(credential.0, Some(&feature), Permission::Read)
            .await?;
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

    /// Get the project the feature is in
    #[oai(
        path = "/features/:feature/project",
        method = "get",
        tag = "ApiTags::Feature"
    )]
    async fn get_feature_project(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// Feature name or id
        feature: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&feature), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetEntityProject {
                    id_or_name: feature.0,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    /// Get all user role mappings
    #[oai(path = "/userroles", method = "get", tag = "ApiTags::Rbac")]
    async fn get_user_roles(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
    ) -> poem::Result<Json<Vec<RbacResponse>>> {
        data.0
            .check_permission(credential.0, Some("global"), Permission::Admin)
            .await?;
        data.0
            .request(opt_seq.0, FeathrApiRequest::GetUserRoles)
            .await
            .into_user_roles()
            .map(Json)
    }

    /// Create an user role mapping
    #[oai(
        path = "/users/:user/userroles/add",
        method = "post",
        tag = "ApiTags::Rbac"
    )]
    async fn add_user_role(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// User name
        user: Path<String>,
        /// Scope of the role, can be a project name or "global"
        project: Query<String>,
        /// Role name
        role: Query<String>,
        /// Reason for the role mapping creation
        reason: Query<String>,
    ) -> poem::Result<Json<String>> {
        data.0
            .check_permission(credential.0, Some("global"), Permission::Admin)
            .await?;
        let resp = data
            .0
            .request(
                opt_seq.0,
                FeathrApiRequest::AddUserRole {
                    user: user.0.parse().map_err(BadRequest)?,
                    project_id_or_name: project.0,
                    role: match role.0.to_lowercase().as_str() {
                        "admin" => Permission::Admin,
                        "consumer" => Permission::Read,
                        "producer" => Permission::Write,
                        _ => {
                            return Err(BadRequest(StringError::new(format!(
                                "invalid role {}",
                                role.0
                            ))))
                        }
                    },
                    requestor: credential.0.to_owned(),
                    reason: reason.0,
                },
            )
            .await;
        match resp {
            registry_api::FeathrApiResponse::Unit => Ok(Json("OK".to_string())),
            registry_api::FeathrApiResponse::Error(e) => Err(e.into()),
            _ => Err(InternalServerError(StringError::new(
                "Internal Server Error",
            ))),
        }
    }

    /// Delete an user role mapping
    #[oai(
        path = "/users/:user/userroles/delete",
        method = "delete",
        tag = "ApiTags::Rbac"
    )]
    async fn delete_user_role(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        /// User name
        user: Path<String>,
        /// Scope of the role, can be a project name or "global"
        project: Query<String>,
        /// Role name
        role: Query<String>,
        /// Reason for the role mapping deletion
        reason: Query<String>,
    ) -> poem::Result<Json<String>> {
        data.0
            .check_permission(credential.0, Some("global"), Permission::Admin)
            .await?;
        let resp = data
            .0
            .request(
                opt_seq.0,
                FeathrApiRequest::DeleteUserRole {
                    user: user.0.parse().map_err(BadRequest)?,
                    project_id_or_name: project.0,
                    role: match role.0.to_lowercase().as_str() {
                        "admin" => Permission::Admin,
                        "consumer" => Permission::Read,
                        "producer" => Permission::Write,
                        _ => {
                            return Err(BadRequest(StringError::new(format!(
                                "invalid role {}",
                                role.0
                            ))))
                        }
                    },
                    requestor: credential.0.to_owned(),
                    reason: reason.0,
                },
            )
            .await;
        match resp {
            registry_api::FeathrApiResponse::Unit => Ok(Json("OK".to_string())),
            registry_api::FeathrApiResponse::Error(e) => Err(e.into()),
            _ => Err(InternalServerError(StringError::new(
                "Internal Server Error",
            ))),
        }
    }
}

fn parse_version<T>(v: T) -> Result<Option<u64>, ApiError>
where
    T: AsRef<str>,
{
    if v.as_ref() == "latest" {
        return Ok(None);
    }
    Ok(Some(v.as_ref().parse().map_err(|_| {
        ApiError::BadRequest(format!("Invalid version spec {}", v.as_ref()))
    })?))
}

#[cfg(test)]
mod tests {
    use super::parse_version;

    #[test]
    fn test_parse_version() {
        assert!(parse_version("").is_err());
        assert!(parse_version("xyz").is_err());
        assert!(parse_version("123xyz").is_err());
        assert!(parse_version("latest").unwrap().is_none());
        assert_eq!(parse_version("1").unwrap(), Some(1));
        assert_eq!(parse_version("42").unwrap(), Some(42));
    }
}
