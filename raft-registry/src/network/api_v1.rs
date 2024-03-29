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
    AnchorDef, AnchorFeatureDef, CreationResponse, DerivedFeatureDef, Entity, EntityLineage,
    FeathrApiRequest, ProjectDef, RbacResponse, SourceDef,
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
pub struct FeathrApiV1;

#[OpenApi]
impl FeathrApiV1 {
    #[oai(path = "/projects", method = "get", tag = "ApiTags::Project")]
    async fn get_projects(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        keyword: Query<Option<String>>,
        page: Query<Option<usize>>,
        limit: Query<Option<usize>>,
    ) -> poem::Result<Json<Vec<String>>> {
        data.0
            .check_permission(credential.0, Some("global"), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjects {
                    keyword: keyword.0,
                    size: limit.0,
                    offset: page.map(|page| (page - 1) * limit.unwrap_or(10)),
                },
            )
            .await
            .into_entity_names()
            .map(Json)
    }

    #[oai(path = "/projects", method = "post", tag = "ApiTags::Project")]
    async fn new_project(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
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

    #[oai(path = "/projects/:project", method = "get", tag = "ApiTags::Project")]
    async fn get_project_lineage(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
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

    #[oai(
        path = "/projects/:project/features",
        method = "get",
        tag = "ApiTags::Project"
    )]
    async fn get_project_features(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        page: Query<Option<usize>>,
        limit: Query<Option<usize>>,
    ) -> poem::Result<Json<Vec<Entity>>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectFeatures {
                    project_id_or_name: project.0,
                    keyword: keyword.0,
                    size: limit.0,
                    offset: page.map(|page| (page - 1) * limit.unwrap_or(10)),
                },
            )
            .await
            .into_entities()
            .map(|es| es.entities)
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/datasources",
        method = "get",
        tag = "ApiTags::DataSource"
    )]
    async fn get_project_datasources(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        page: Query<Option<usize>>,
        limit: Query<Option<usize>>,
    ) -> poem::Result<Json<Vec<Entity>>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDataSources {
                    project_id_or_name: project.0,
                    keyword: keyword.0,
                    size: limit.0,
                    offset: page.map(|page| (page - 1) * limit.unwrap_or(10)),
                },
            )
            .await
            .into_entities()
            .map(|es| es.entities)
            .map(Json)
    }

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
        project: Path<String>,
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
        project: Path<String>,
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
        project: Path<String>,
        keyword: Query<Option<String>>,
        page: Query<Option<usize>>,
        limit: Query<Option<usize>>,
    ) -> poem::Result<Json<Vec<Entity>>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectAnchors {
                    project_id_or_name: project.0,
                    keyword: keyword.0,
                    size: limit.0,
                    offset: page.map(|page| (page - 1) * limit.unwrap_or(10)),
                },
            )
            .await
            .into_entities()
            .map(|es| es.entities)
            .map(Json)
    }

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
        project: Path<String>,
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
        project: Path<String>,
        anchor: Path<String>,
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

    #[oai(path = "/features/:feature", method = "get", tag = "ApiTags::Feature")]
    async fn get_feature(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
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
        user: Path<String>,
        project: Query<String>,
        role: Query<String>,
        reason: Query<String>,
    ) -> poem::Result<Json<String>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Admin)
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
        user: Path<String>,
        project: Query<String>,
        role: Query<String>,
        reason: Query<String>,
    ) -> poem::Result<Json<String>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Admin)
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
