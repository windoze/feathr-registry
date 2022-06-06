use std::collections::HashSet;

use async_trait::async_trait;
use common_utils::{set, Blank};
use log::debug;
use registry_provider::{Edge, EdgeProperty, EntityProperty, RegistryError, RegistryProvider};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AnchorDef, AnchorFeatureDef, ApiError, DerivedFeatureDef, Entities, Entity, EntityLineage,
    IntoApiResult, ProjectDef, SourceDef,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FeathrApiRequest {
    GetProjects {
        keyword: Option<String>,
        size: Option<usize>,
        offset: Option<usize>,
    },
    GetProject {
        id_or_name: String,
    },
    GetProjectLineage {
        id_or_name: String,
    },
    GetProjectFeatures {
        project_id_or_name: String,
        keyword: Option<String>,
        size: Option<usize>,
        offset: Option<usize>,
    },
    CreateProject {
        definition: ProjectDef,
    },
    GetProjectDataSources {
        project_id_or_name: String,
        keyword: Option<String>,
        size: Option<usize>,
        offset: Option<usize>,
    },
    GetProjectDataSource {
        project_id_or_name: String,
        id_or_name: String,
    },
    CreateProjectDataSource {
        project_id_or_name: String,
        definition: SourceDef,
    },
    GetProjectAnchors {
        project_id_or_name: String,
        keyword: Option<String>,
        size: Option<usize>,
        offset: Option<usize>,
    },
    GetProjectAnchor {
        project_id_or_name: String,
        id_or_name: String,
    },
    CreateProjectAnchor {
        project_id_or_name: String,
        definition: AnchorDef,
    },
    GetProjectDerivedFeatures {
        project_id_or_name: String,
        keyword: Option<String>,
        size: Option<usize>,
        offset: Option<usize>,
    },
    GetProjectDerivedFeature {
        project_id_or_name: String,
        id_or_name: String,
    },
    CreateProjectDerivedFeature {
        project_id_or_name: String,
        definition: DerivedFeatureDef,
    },
    GetAnchorFeatures {
        project_id_or_name: String,
        anchor_id_or_name: String,
        keyword: Option<String>,
        size: Option<usize>,
        offset: Option<usize>,
    },
    GetAnchorFeature {
        project_id_or_name: String,
        anchor_id_or_name: String,
        id_or_name: String,
    },
    CreateAnchorFeature {
        project_id_or_name: String,
        anchor_id_or_name: String,
        definition: AnchorFeatureDef,
    },
    GetFeature {
        id_or_name: String,
    },
    GetFeatureLineage {
        id_or_name: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FeathrApiResponse {
    Error(ApiError),

    Unit,
    Uuid(Uuid),
    Entity(Entity),
    Entities(Entities),
    EntityLineage(EntityLineage),
}

impl FeathrApiResponse {
    pub fn into_uuid(self) -> poem::Result<Uuid> {
        match self {
            FeathrApiResponse::Error(e) => Err(e.into()),
            FeathrApiResponse::Uuid(v) => Ok(v),
            _ => panic!("Shouldn't reach here"),
        }
    }
    pub fn into_entity(self) -> poem::Result<Entity> {
        match self {
            FeathrApiResponse::Error(e) => Err(e.into()),
            FeathrApiResponse::Entity(v) => Ok(v),
            _ => panic!("Shouldn't reach here"),
        }
    }

    pub fn into_entities(self) -> poem::Result<Entities> {
        match self {
            FeathrApiResponse::Error(e) => Err(e.into()),
            FeathrApiResponse::Entities(v) => Ok(v),
            _ => panic!("Shouldn't reach here"),
        }
    }
    pub fn into_lineage(self) -> poem::Result<EntityLineage> {
        match self {
            FeathrApiResponse::Error(e) => Err(e.into()),
            FeathrApiResponse::EntityLineage(v) => Ok(v),
            _ => panic!("Shouldn't reach here"),
        }
    }
}

impl From<Result<(), RegistryError>> for FeathrApiResponse {
    fn from(v: Result<(), RegistryError>) -> Self {
        match v {
            Ok(_) => Self::Unit,
            Err(e) => Self::Error(e.into()),
        }
    }
}

impl From<Result<Uuid, RegistryError>> for FeathrApiResponse {
    fn from(v: Result<Uuid, RegistryError>) -> Self {
        match v {
            Ok(v) => Self::Uuid(v),
            Err(e) => Self::Error(e.into()),
        }
    }
}

impl From<Result<registry_provider::Entity<EntityProperty>, RegistryError>> for FeathrApiResponse {
    fn from(v: Result<registry_provider::Entity<EntityProperty>, RegistryError>) -> Self {
        match v {
            Ok(v) => Self::Entity(v.into()),
            Err(e) => Self::Error(e.into()),
        }
    }
}

impl From<Result<Entity, RegistryError>> for FeathrApiResponse {
    fn from(v: Result<Entity, RegistryError>) -> Self {
        match v {
            Ok(v) => Self::Entity(v),
            Err(e) => Self::Error(e.into()),
        }
    }
}

impl From<Result<Vec<registry_provider::Entity<EntityProperty>>, RegistryError>>
    for FeathrApiResponse
{
    fn from(v: Result<Vec<registry_provider::Entity<EntityProperty>>, RegistryError>) -> Self {
        match v {
            Ok(v) => Self::Entities(v.into_iter().collect()),
            Err(e) => Self::Error(e.into()),
        }
    }
}

impl From<Result<Entities, RegistryError>> for FeathrApiResponse {
    fn from(v: Result<Entities, RegistryError>) -> Self {
        match v {
            Ok(v) => Self::Entities(v),
            Err(e) => Self::Error(e.into()),
        }
    }
}

impl
    From<
        Result<
            (
                Vec<registry_provider::Entity<EntityProperty>>,
                Vec<Edge<EdgeProperty>>,
            ),
            RegistryError,
        >,
    > for FeathrApiResponse
{
    fn from(
        v: Result<
            (
                Vec<registry_provider::Entity<EntityProperty>>,
                Vec<Edge<EdgeProperty>>,
            ),
            RegistryError,
        >,
    ) -> Self {
        match v {
            Ok(v) => Self::EntityLineage(v.into()),
            Err(e) => Self::Error(e.into()),
        }
    }
}

impl From<Result<EntityLineage, RegistryError>> for FeathrApiResponse {
    fn from(v: Result<EntityLineage, RegistryError>) -> Self {
        match v {
            Ok(v) => Self::EntityLineage(v),
            Err(e) => Self::Error(e.into()),
        }
    }
}

#[async_trait]
pub trait FeathrApiProvider: Sync + Send {
    async fn request(&mut self, request: FeathrApiRequest) -> FeathrApiResponse;
}

#[async_trait]
impl<T> FeathrApiProvider for T
where
    T: RegistryProvider<EntityProperty, EdgeProperty> + Sync + Send,
{
    async fn request(&mut self, request: FeathrApiRequest) -> FeathrApiResponse {

        async fn get_id<T>(t: &T, id_or_name: String) -> Result<Uuid, RegistryError>
        where
            T: RegistryProvider<EntityProperty, EdgeProperty>,
        {
            match Uuid::parse_str(&id_or_name) {
                Ok(id) => Ok(id),
                Err(_) => t.get_entity_id(&id_or_name).await,
            }
        }

        async fn get_name<T>(t: &T, uuid: Uuid) -> Result<String, RegistryError>
        where
            T: RegistryProvider<EntityProperty, EdgeProperty>,
        {
            t.get_entity_name(uuid).await
        }

        async fn get_child_id<T>(
            t: &T,
            parent_id_or_name: String,
            child_id_or_name: String,
        ) -> Result<(Uuid, Uuid), RegistryError>
        where
            T: RegistryProvider<EntityProperty, EdgeProperty>,
        {
            debug!("Parent name: {}", parent_id_or_name);
            debug!("Child name: {}", child_id_or_name);
            let parent_id = get_id(t, parent_id_or_name).await?;
            let child_id = match get_id(t, child_id_or_name.clone()).await {
                Ok(id) => id,
                Err(_) => {
                    let project_name = get_name(t, parent_id).await?;
                    get_id(t, format!("{}__{}", project_name, child_id_or_name)).await?
                }
            };
            Ok((parent_id, child_id))
        }

        async fn search_entities<T>(
            t: &T,
            keyword: Option<String>,
            size: Option<usize>,
            offset: Option<usize>,
            types: HashSet<registry_provider::EntityType>,
            scope: Option<Uuid>,
        ) -> Result<Vec<registry_provider::Entity<EntityProperty>>, RegistryError>
        where
            T: RegistryProvider<EntityProperty, EdgeProperty>,
        {
            t.search_entity(
                &keyword.unwrap_or_default(),
                types,
                scope,
                size.unwrap_or(100),
                offset.unwrap_or(0),
            )
            .await
        }

        async fn search_children<T>(
            t: &T,
            id_or_name: String,
            keyword: Option<String>,
            size: Option<usize>,
            offset: Option<usize>,
            types: HashSet<registry_provider::EntityType>,
        ) -> Result<Vec<registry_provider::Entity<EntityProperty>>, RegistryError>
        where
            T: RegistryProvider<EntityProperty, EdgeProperty>,
        {
            debug!("Project name: {}", id_or_name);
            let scope_id = get_id(t, id_or_name).await?;

            if keyword.is_blank() {
                t.get_children(scope_id, types).await.into()
            } else {
                search_entities(t, keyword, size, offset, types, Some(scope_id))
                    .await
                    .into()
            }
        }

        async fn handle_request<T>(
            this: &mut T,
            request: FeathrApiRequest,
        ) -> Result<FeathrApiResponse, ApiError>
        where
            T: RegistryProvider<EntityProperty, EdgeProperty>,
        {
            Ok(match request {
                FeathrApiRequest::GetProjects {
                    keyword,
                    size,
                    offset,
                } => {
                    if keyword.is_blank() {
                        this.get_entry_points().await.into()
                    } else {
                        search_entities(
                            this,
                            keyword,
                            size,
                            offset,
                            set![registry_provider::EntityType::Project],
                            None,
                        )
                        .await
                        .into()
                    }
                }
                FeathrApiRequest::GetProject { id_or_name } => this
                    .get_entity_by_id_or_qualified_name(&id_or_name)
                    .await
                    .into(),
                FeathrApiRequest::GetProjectLineage { id_or_name } => {
                    debug!("Project name: {}", id_or_name);

                    this.get_project(&id_or_name).await.into()
                }
                FeathrApiRequest::GetProjectFeatures {
                    project_id_or_name,
                    keyword,
                    size,
                    offset,
                } => {
                    debug!("Project name: {}", project_id_or_name);
                    search_children(
                        this,
                        project_id_or_name,
                        keyword,
                        size,
                        offset,
                        set![
                            registry_provider::EntityType::AnchorFeature,
                            registry_provider::EntityType::DerivedFeature
                        ],
                    )
                    .await
                    .into()
                }
                FeathrApiRequest::CreateProject { mut definition } => {
                    definition.qualified_name = definition.name.clone();
                    let guid = Uuid::new_v4();
                    this.new_project(guid, &definition.try_into()?).await.into()
                }
                FeathrApiRequest::GetProjectDataSources {
                    project_id_or_name,
                    keyword,
                    size,
                    offset,
                } => {
                    debug!("Project name: {}", project_id_or_name);
                    search_children(
                        this,
                        project_id_or_name,
                        keyword,
                        size,
                        offset,
                        set![registry_provider::EntityType::Source],
                    )
                    .await
                    .into()
                }
                FeathrApiRequest::GetProjectDataSource {
                    project_id_or_name,
                    id_or_name,
                } => {
                    let (_, source_id) = get_child_id(this, project_id_or_name, id_or_name).await?;
                    this.get_entity(source_id).await.into()
                }
                FeathrApiRequest::CreateProjectDataSource {
                    project_id_or_name,
                    mut definition,
                } => {
                    let project_id = get_id(this, project_id_or_name).await?;
                    let project_name = get_name(this, project_id).await?;
                    definition.qualified_name = format!("{}__{}", project_name, definition.name);
                    let guid = Uuid::new_v4();
                    this.new_source(project_id, guid, &definition.try_into()?)
                        .await
                        .into()
                }
                FeathrApiRequest::GetProjectAnchors {
                    project_id_or_name,
                    keyword,
                    size,
                    offset,
                } => {
                    debug!("Project name: {}", project_id_or_name);
                    search_children(
                        this,
                        project_id_or_name,
                        keyword,
                        size,
                        offset,
                        set![registry_provider::EntityType::Anchor],
                    )
                    .await
                    .into()
                }
                FeathrApiRequest::GetProjectAnchor {
                    project_id_or_name,
                    id_or_name,
                } => {
                    let (_, anchor_id) = get_child_id(this, project_id_or_name, id_or_name).await?;
                    this.get_entity(anchor_id).await.into()
                }
                FeathrApiRequest::CreateProjectAnchor {
                    project_id_or_name,
                    mut definition,
                } => {
                    let project_id = get_id(this, project_id_or_name).await?;
                    let project_name = get_name(this, project_id).await?;
                    definition.qualified_name = format!("{}__{}", project_name, definition.name);
                    let guid = Uuid::new_v4();
                    this.new_anchor(project_id, guid, &definition.try_into()?)
                        .await
                        .into()
                }
                FeathrApiRequest::GetProjectDerivedFeatures {
                    project_id_or_name,
                    keyword,
                    size,
                    offset,
                } => {
                    debug!("Project name: {}", project_id_or_name);
                    search_children(
                        this,
                        project_id_or_name,
                        keyword,
                        size,
                        offset,
                        set![registry_provider::EntityType::DerivedFeature],
                    )
                    .await
                    .into()
                }
                FeathrApiRequest::GetProjectDerivedFeature {
                    project_id_or_name,
                    id_or_name,
                } => {
                    let (_, feature_id) =
                        get_child_id(this, project_id_or_name, id_or_name).await?;
                    this.get_entity(feature_id).await.into()
                }
                FeathrApiRequest::CreateProjectDerivedFeature {
                    project_id_or_name,
                    mut definition,
                } => {
                    let project_id = get_id(this, project_id_or_name).await?;
                    let project_name = get_name(this, project_id).await?;
                    definition.qualified_name = format!("{}__{}", project_name, definition.name);
                    let guid = Uuid::new_v4();
                    this.new_derived_feature(project_id, guid, &definition.try_into()?)
                        .await
                        .into()
                }
                FeathrApiRequest::GetAnchorFeatures {
                    project_id_or_name,
                    anchor_id_or_name,
                    keyword,
                    size,
                    offset,
                } => {
                    let (_, anchor_id) =
                        get_child_id(this, project_id_or_name, anchor_id_or_name).await?;
                    search_children(
                        this,
                        anchor_id.to_string(),
                        keyword,
                        size,
                        offset,
                        set![registry_provider::EntityType::AnchorFeature],
                    )
                    .await
                    .into()
                }
                FeathrApiRequest::GetAnchorFeature {
                    project_id_or_name,
                    anchor_id_or_name,
                    id_or_name,
                } => {
                    let (_, anchor_id) =
                        get_child_id(this, project_id_or_name, anchor_id_or_name).await?;
                    let (_, feature_id) =
                        get_child_id(this, anchor_id.to_string(), id_or_name).await?;
                    this.get_entity(feature_id).await.into()
                }
                FeathrApiRequest::CreateAnchorFeature {
                    project_id_or_name,
                    anchor_id_or_name,
                    definition,
                } => {
                    let (project_id, anchor_id) =
                        get_child_id(this, project_id_or_name, anchor_id_or_name).await?;
                    let id = Uuid::new_v4();
                    this.new_anchor_feature(project_id, anchor_id, id, &definition.try_into()?)
                        .await
                        .into()
                }
                FeathrApiRequest::GetFeature { id_or_name } => this
                    .get_entity_by_id_or_qualified_name(&id_or_name)
                    .await
                    .into(),
                FeathrApiRequest::GetFeatureLineage { id_or_name } => {
                    debug!("Feature name: {}", id_or_name);
                    let id = get_id(this, id_or_name).await?;
                    let (up_entities, up_edges) = this
                        .bfs(id, registry_provider::EdgeType::Consumes, 100)
                        .await
                        .map_api_error()?;
                    let (down_entities, down_edges) = this
                        .bfs(id, registry_provider::EdgeType::Produces, 100)
                        .await
                        .map_api_error()?;
                    Ok((
                        up_entities
                            .into_iter()
                            .chain(down_entities.into_iter())
                            .collect::<Vec<_>>(),
                        up_edges
                            .into_iter()
                            .chain(down_edges.into_iter())
                            .collect::<Vec<_>>(),
                    ))
                    .into()
                }
            })
        }

        match handle_request(self, request).await {
            Ok(v) => v,
            Err(e) => FeathrApiResponse::Error(e),
        }
    }
}
