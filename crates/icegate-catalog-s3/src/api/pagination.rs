//! Cursor pagination for the catalog collection endpoints.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

use super::config::CatalogApiConfig;
use super::dto::CollectionQuery;
use super::error::ApiError;

const PAGE_TOKEN_VERSION: u8 = 1;

/// Opaque continuation-token payload.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct PageToken {
    version: u8,
    operation: PageOperation,
    scope: Option<Vec<String>>,
    last_identifier: PageIdentifier,
}

/// Collection operation encoded in a page token.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum PageOperation {
    /// Namespace list operation.
    Namespaces,
    /// Table list operation.
    Tables,
}

/// Last identifier encoded in a page token.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct PageIdentifier {
    pub(super) namespace: Vec<String>,
    pub(super) name: Option<String>,
}

/// Cut one page out of `items`, ordered by `identifier`, per the Iceberg
/// pagination contract of the pinned spec revision.
///
/// `pageToken` is the only signal that a client wants pagination: without it the
/// whole sorted collection is returned with a `null` continuation token, even
/// when `pageSize` is present. An empty `pageToken` opens the first page.
pub(super) fn paginate<T, F>(
    mut items: Vec<T>,
    query: &CollectionQuery,
    operation: PageOperation,
    scope: Option<Vec<String>>,
    config: &CatalogApiConfig,
    identifier: F,
) -> Result<(Vec<T>, Option<String>), ApiError>
where
    F: Fn(&T) -> PageIdentifier,
{
    // Sorted in every branch: the cursor is the last identifier of a page, so a
    // stable total order is what makes the continuation loop lossless — and an
    // unpaginated response must not change order just because it fits one page.
    items.sort_by_key(&identifier);

    let Some(page_token) = query.page_token.as_deref() else {
        return Ok((items, None));
    };

    let effective_page_size = query
        .page_size
        .unwrap_or_else(|| config.default_page_size())
        .min(config.max_page_size())
        .get();
    let start = if page_token.is_empty() {
        0
    } else {
        let token = decode_page_token(page_token, operation, scope.as_ref())?;
        items.partition_point(|item| identifier(item) <= token.last_identifier)
    };
    let end = start.saturating_add(effective_page_size).min(items.len());
    let has_more = end < items.len();
    let page = items.drain(start..end).collect::<Vec<_>>();
    let next_page_token = if has_more {
        let Some(last_identifier) = page.last().map(&identifier) else {
            return Err(ApiError::Internal);
        };
        encode_page_token(&PageToken {
            version: PAGE_TOKEN_VERSION,
            operation,
            scope,
            last_identifier,
        })?
        .into()
    } else {
        None
    };
    Ok((page, next_page_token))
}

fn encode_page_token(token: &PageToken) -> Result<String, ApiError> {
    serde_json::to_vec(&token)
        .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
        .map_err(|_| ApiError::Internal)
}

fn decode_page_token(
    encoded: &str,
    operation: PageOperation,
    scope: Option<&Vec<String>>,
) -> Result<PageToken, ApiError> {
    let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| ApiError::BadRequest {
        message: "pageToken is invalid".to_string(),
    })?;
    let token: PageToken = serde_json::from_slice(&bytes).map_err(|_| ApiError::BadRequest {
        message: "pageToken is invalid".to_string(),
    })?;
    if token.version != PAGE_TOKEN_VERSION || token.operation != operation || token.scope.as_ref() != scope {
        return Err(ApiError::BadRequest {
            message: "pageToken does not match this request".to_string(),
        });
    }
    Ok(token)
}
