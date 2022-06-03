mod error;
mod fts;
mod models;
mod registry;
mod operators;

pub use error::RegistryError;
pub use fts::*;
pub use models::*;
pub use registry::*;
pub use operators::*;

pub trait SerializableRegistry {
    fn take_snapshot(&self) -> Result<Vec<u8>, RegistryError>;
    fn install_snapshot(&mut self) -> Result<(), RegistryError>;
}