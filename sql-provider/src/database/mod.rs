use registry_provider::{EdgeProperty, EntityProperty, Entity, Edge};

use crate::Registry;

#[cfg(feature = "mssql")]
mod mssql;

#[cfg(feature = "ossdmbs")]
mod sqlx;

pub async fn load_registry() -> Result<Registry<EntityProperty, EdgeProperty>, anyhow::Error> {
    #[cfg(feature = "ossdmbs")]
    if sqlx::validate_condition() {
        return sqlx::load_registry().await;
    }
    #[cfg(feature = "mssql")]
    if mssql::validate_condition() {
        return mssql::load_registry().await;
    }
    anyhow::bail!("Unable to load registry")
}

pub fn attach_storage(registry: &mut Registry<EntityProperty, EdgeProperty>) {
    #[cfg(feature = "ossdmbs")]
    todo!();

    #[cfg(feature = "mssql")]
    mssql::attach_storage(registry);
}

pub async fn load_content(
) -> Result<(Vec<Entity<EntityProperty>>, Vec<Edge<EdgeProperty>>), anyhow::Error> {
    #[cfg(feature = "ossdmbs")]
    todo!();

    #[cfg(feature = "mssql")]
    mssql::load_content().await
}
