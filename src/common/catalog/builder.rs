//! Catalog builder factory

use super::{CatalogConfig, CatalogBackend};
use crate::common::Result;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::{Catalog, CatalogBuilder as IcebergCatalogBuilder};
use iceberg_catalog_rest::RestCatalogBuilder;
use std::collections::HashMap;
use std::sync::Arc;

/// Factory for creating catalog instances from configuration
pub struct CatalogBuilder;

impl CatalogBuilder {
    /// Create a catalog from configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be created
    pub async fn from_config(config: &CatalogConfig) -> Result<Arc<dyn Catalog>> {
        match &config.backend {
            CatalogBackend::Memory => Self::create_memory_catalog(config).await,
            CatalogBackend::Rest { uri } => Self::create_rest_catalog(config, uri).await,
        }
    }

    /// Create a memory catalog
    async fn create_memory_catalog(config: &CatalogConfig) -> Result<Arc<dyn Catalog>> {
        let mut properties = HashMap::new();
        properties.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), config.warehouse.clone());

        // Add any additional properties from config
        for (key, value) in &config.properties {
            properties.insert(key.clone(), value.clone());
        }

        let catalog = MemoryCatalogBuilder::default()
            .load("icegate", properties)
            .await?;

        tracing::info!(
            warehouse = %config.warehouse,
            "Created Memory catalog"
        );

        Ok(Arc::new(catalog))
    }

    /// Create a REST catalog
    async fn create_rest_catalog(config: &CatalogConfig, uri: &str) -> Result<Arc<dyn Catalog>> {
        // Build REST catalog properties
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), uri.to_string());
        properties.insert("warehouse".to_string(), config.warehouse.clone());

        // Add any additional properties from config
        for (key, value) in &config.properties {
            properties.insert(key.clone(), value.clone());
        }

        // Create REST catalog using RestCatalogBuilder
        let catalog = RestCatalogBuilder::default().load("rest", properties).await?;

        tracing::info!(
            uri = %uri,
            warehouse = %config.warehouse,
            "Created REST catalog"
        );

        Ok(Arc::new(catalog))
    }
}
