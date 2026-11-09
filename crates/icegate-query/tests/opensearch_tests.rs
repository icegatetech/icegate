//! OpenSearch-compatible API integration tests. Every request is a literal in the test, and every
//! expected result is stated there.
//!
//! - `aggregations`: `date_histogram`, `terms` and the bucket limit
//! - `answers`: readers of `_search` answers shared by the data-index modules
//! - `cat`: `_cat/indices` and `_cat/plugins`
//! - `cluster`: the cluster-level endpoints
//! - `dashboards_migration`: Dashboards-style saved-object call sequences (first boot, no-op and
//!   reindexing upgrades, export and import) and the snapshot bound during a migration
//! - `data`: table rows with explicit values, written through the production writer
//! - `discover`: Discover-style document table, histogram, single-document and context views
//! - `fields`: `_field_caps`, `_mapping` and `_mapping/field`
//! - `grafana_logs`: Grafana-logs-style `_msearch` logs query, logs volume and variables
//! - `grafana_metrics`: metrics-editor-style aggregation and variable requests
//! - `harness`: the listener over a temporary warehouse, and a reqwest client
//! - `indices`: `_resolve/index` and `<index>/_settings`
//! - `listener`: the listener's start-up
//! - `msearch`: `_msearch` over HTTP: items, their order, statuses and per-item errors
//! - `ppl`: `/_plugins/_ppl`: commands, functions, the jdbc response, the PPL error envelope
//! - `ppl_clients`: client-shaped PPL requests and PPL tenant isolation
//! - `ppl_endpoints`: cluster settings, `SHOW TABLES`, the PPL grammar bundle and the plugin stubs
//! - `ppl_formats`: the `viz`, `csv` and `raw` formats, `_explain`, and the scan pushdowns it shows
//! - `ppl_trust_delta`: PPL `join` beyond the core commands
//! - `profiles`: the Data Prepper span profile over real HTTP
//! - `requests`: JSON and NDJSON request helpers shared by the system-index and data-index modules
//! - `rustfs_conditional`: the object-store guarantees the system-index store builds on
//! - `saved_objects`: system-index document endpoints
//! - `search`: data-index `_search`: lowering, hits, limits, tenant isolation
//! - `search_tenant`: tenant isolation of `_search`, `_msearch`, `ids`, context views,
//!   aggregations and unions
//! - `system_indices`: index-level system-index endpoints and their tenant boundary
//! - `system_store_rustfs`: the system-index store against RustFS through the production constructor
//! - `trust_parity`: trust's statements, the SQL form over Flight SQL against the PPL form
//! - `visualize`: Visualize-style aggregation requests, one per acceptance type

mod opensearch;
