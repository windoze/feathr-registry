use registry_provider::{EntityProperty, EdgeProperty};

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