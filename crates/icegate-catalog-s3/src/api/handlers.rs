//! REST endpoint handlers.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use iceberg::Catalog;

use super::dto::{
    CatalogConfigResponse, CollectionQuery, ListNamespacesResponse, ListTablesResponse, LoadTableQuery,
    LoadTableResponse, NamespaceListQuery, NamespacePath, NamespaceResponse, TableIdentifierResponse, TablePath,
};
use super::error::ApiError;
use super::extract::{ApiPath, ApiQuery};
use super::identifier::{parse_namespace, parse_table, to_namespace_parts};
use super::pagination::{PageIdentifier, PageOperation, paginate};
use super::state::CatalogApiState;
use super::table_metadata::{
    insert_etag, metadata_etag, request_matches_etag, serialize_metadata, validate_access_delegation,
    validate_referenced_by,
};

/// Serve the catalog configuration precomputed for this router.
pub(super) async fn get_config(
    State(state): State<CatalogApiState>,
    ApiQuery(query): ApiQuery<HashMap<String, String>>,
) -> Result<Json<Arc<CatalogConfigResponse>>, ApiError> {
    // Single-warehouse server: a client naming any other warehouse is asking for
    // a catalog this process does not serve. A trailing slash does not name
    // another warehouse — the spec's own `getConfig` example writes
    // `s3://bucket/warehouse/`, while the canonical URI never carries one — so
    // both sides are compared without it.
    if let Some(warehouse) = query.get("warehouse")
        && warehouse.trim_end_matches('/') != state.config.warehouse_uri().trim_end_matches('/')
    {
        return Err(ApiError::NoSuchWarehouse);
    }
    Ok(Json(Arc::clone(&state.catalog_config_response)))
}

/// List namespaces with cursor pagination.
pub(super) async fn list_namespaces(
    State(state): State<CatalogApiState>,
    ApiQuery(query): ApiQuery<NamespaceListQuery>,
) -> Result<Json<ListNamespacesResponse>, ApiError> {
    let parent = query
        .parent
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(parse_namespace)
        .transpose()?;
    let namespaces = state.catalog.list_namespaces(parent.as_ref()).await.map_err(ApiError::from)?;
    let scope = parent.as_ref().map(to_namespace_parts);
    let (page, next_page_token) = paginate(
        namespaces,
        &query.collection,
        PageOperation::Namespaces,
        scope,
        &state.config,
        |namespace| PageIdentifier {
            namespace: to_namespace_parts(namespace),
            name: None,
        },
    )?;
    Ok(Json(ListNamespacesResponse {
        namespaces: page.iter().map(to_namespace_parts).collect(),
        next_page_token,
    }))
}

/// Load one namespace.
pub(super) async fn get_namespace(
    State(state): State<CatalogApiState>,
    ApiPath(NamespacePath { namespace }): ApiPath<NamespacePath>,
) -> Result<Json<NamespaceResponse>, ApiError> {
    let namespace = parse_namespace(&namespace)?;
    let result = state.catalog.get_namespace(&namespace).await.map_err(ApiError::from)?;
    Ok(Json(NamespaceResponse {
        namespace: to_namespace_parts(result.name()),
        properties: result.properties().clone(),
    }))
}

/// Return an empty status response for a namespace existence check.
pub(super) async fn head_namespace(
    State(state): State<CatalogApiState>,
    ApiPath(NamespacePath { namespace }): ApiPath<NamespacePath>,
) -> Result<StatusCode, ApiError> {
    let namespace = parse_namespace(&namespace)?;
    if state.catalog.namespace_exists(&namespace).await.map_err(ApiError::from)? {
        return Ok(StatusCode::NO_CONTENT);
    }
    Err(ApiError::NoSuchNamespace)
}

/// List active tables within one namespace with cursor pagination.
pub(super) async fn list_tables(
    State(state): State<CatalogApiState>,
    ApiPath(NamespacePath { namespace }): ApiPath<NamespacePath>,
    ApiQuery(query): ApiQuery<CollectionQuery>,
) -> Result<Json<ListTablesResponse>, ApiError> {
    let namespace = parse_namespace(&namespace)?;
    let tables = state.catalog.list_tables(&namespace).await.map_err(ApiError::from)?;
    let scope = Some(to_namespace_parts(&namespace));
    let (page, next_page_token) = paginate(tables, &query, PageOperation::Tables, scope, &state.config, |table| {
        PageIdentifier {
            namespace: to_namespace_parts(table.namespace()),
            name: Some(table.name().to_string()),
        }
    })?;
    Ok(Json(ListTablesResponse {
        identifiers: page
            .iter()
            .map(|table| TableIdentifierResponse {
                namespace: to_namespace_parts(table.namespace()),
                name: table.name().to_string(),
            })
            .collect(),
        next_page_token,
    }))
}

/// Load Iceberg table metadata with snapshot selection and HTTP validators.
pub(super) async fn load_table(
    State(state): State<CatalogApiState>,
    ApiPath(TablePath { namespace, table }): ApiPath<TablePath>,
    ApiQuery(query): ApiQuery<LoadTableQuery>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let namespace = parse_namespace(&namespace)?;
    let table = parse_table(&namespace, &table)?;
    validate_referenced_by(raw_query.as_deref())?;
    validate_access_delegation(&headers)?;

    let table = state.catalog.load_table(&table).await.map_err(ApiError::from)?;
    let selection = query.snapshots.unwrap_or_default();
    let metadata_location = table.metadata_location_result().map_err(ApiError::from)?.to_string();
    let etag = metadata_etag(&metadata_location, selection);
    if request_matches_etag(&headers, &etag)? {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        insert_etag(response.headers_mut(), &etag)?;
        return Ok(response);
    }

    let metadata = serialize_metadata(table.metadata(), selection)?;
    let mut response = Json(LoadTableResponse {
        metadata_location,
        metadata,
        config: HashMap::new(),
        storage_credentials: None,
    })
    .into_response();
    insert_etag(response.headers_mut(), &etag)?;
    Ok(response)
}

/// Return an empty status response for an active table existence check.
pub(super) async fn head_table(
    State(state): State<CatalogApiState>,
    ApiPath(TablePath { namespace, table }): ApiPath<TablePath>,
) -> Result<StatusCode, ApiError> {
    let namespace = parse_namespace(&namespace)?;
    let table = parse_table(&namespace, &table)?;
    if state.catalog.table_exists(&table).await.map_err(ApiError::from)? {
        return Ok(StatusCode::NO_CONTENT);
    }
    Err(ApiError::NoSuchTable)
}
