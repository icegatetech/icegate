//! Thin wrapper around `datafusion_flight_sql_server::service::FlightSqlService`.
//!
//! The upstream service returns `Unimplemented` for the standard
//! Flight SQL `BeginTransaction` / `EndTransaction` actions, which
//! breaks BI/SQL clients that issue them unconditionally (`DuckDB`'s
//! `duckhog` extension, some JDBC drivers, ADBC). Because IceGate is
//! strictly read-only, we can safely return no-op stubs: clients see a
//! synthetic transaction handle on begin and a successful close on
//! end, while no transactional semantics are actually engaged.
//!
//! The wrapper otherwise delegates every Flight SQL trait method to
//! the upstream service so its real implementations (statement
//! execution, prepared statements, catalog/schema introspection, etc.)
//! continue to run.
//!
//! Every method is instrumented with a `tracing` span named
//! `flight_sql.<method>` carrying the tenant identifier (and the SQL
//! text for query-bearing methods). The upstream crate does not emit
//! any spans of its own, so without this layer the only signal in
//! traces would be the outer tonic gRPC span.

use std::pin::Pin;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionBeginSavepointRequest, ActionBeginSavepointResult, ActionBeginTransactionRequest,
    ActionBeginTransactionResult, ActionCancelQueryRequest, ActionCancelQueryResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult,
    ActionCreatePreparedSubstraitPlanRequest, ActionEndSavepointRequest, ActionEndTransactionRequest, Any, Command,
    CommandGetCatalogs, CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementIngest, CommandStatementQuery,
    CommandStatementSubstraitPlan, CommandStatementUpdate, DoPutPreparedStatementResult, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    Action, ActionType, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    Ticket,
};
use datafusion_flight_sql_server::service::FlightSqlService as InnerService;
use futures::Stream;
use icegate_common::DEFAULT_TENANT_ID;
use prost::bytes::Bytes;
use tonic::{Request, Response, Status, Streaming};

use super::tenant_id::resolve_tenant_id;

/// Synthetic transaction handle returned by [`do_action_begin_transaction`].
///
/// Opaque to the client — they only need a non-empty `Bytes` value to
/// echo back on commit/rollback. Since we don't track transactions
/// server-side, any handle they send to `end_transaction` is accepted
/// regardless.
const NOOP_TRANSACTION_ID: &[u8] = b"icegate-noop-txn";

/// `FlightSqlService` impl wrapping the upstream `datafusion-flight-sql-server`
/// service with read-only transaction stubs.
///
/// All trait methods other than `do_action_begin_transaction` and
/// `do_action_end_transaction` forward to the inner service unchanged.
pub(crate) struct IceGateFlightSqlService {
    inner: InnerService,
}

impl IceGateFlightSqlService {
    /// Wrap an existing upstream `FlightSqlService`.
    pub(crate) const fn new(inner: InnerService) -> Self {
        Self { inner }
    }
}

/// Stamp the current span's `tenant_id` field by resolving the gRPC
/// metadata via the shared [`resolve_tenant_id`] helper.
///
/// When the resolver falls back to [`DEFAULT_TENANT_ID`], emit a single
/// debug event listing every ASCII metadata key in the request. This
/// is the diagnostic clients need to figure out what their gRPC
/// framework actually puts on the wire (`DuckDB` `duckhog`, JDBC drivers
/// and ADBC each serialise URL/DSN parameters into headers in their
/// own way). Values are intentionally not logged because some keys
/// carry auth tokens.
fn record_tenant_id<T>(req: &Request<T>) {
    let tenant = resolve_tenant_id(req.metadata());
    tracing::Span::current().record("tenant_id", tenant.as_str());
    if tenant == DEFAULT_TENANT_ID {
        let metadata = req.metadata();
        let keys: Vec<&str> = metadata
            .iter()
            .filter_map(|kv| match kv {
                tonic::metadata::KeyAndValueRef::Ascii(key, _) => Some(key.as_str()),
                tonic::metadata::KeyAndValueRef::Binary(_, _) => None,
            })
            .collect();
        tracing::debug!(
            metadata_keys = ?keys,
            "no valid tenant id in gRPC metadata; falling back to `{DEFAULT_TENANT_ID}` tenant"
        );
    }
}

type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
type DoActionStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightSqlService for IceGateFlightSqlService {
    type FlightService = Self;

    // ─── Overrides ─────────────────────────────────────────────────────

    #[tracing::instrument(name = "flight_sql.do_action_begin_transaction", skip_all, fields(tenant_id))]
    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        record_tenant_id(&request);
        // Read-only server: no actual transaction is started. Return a
        // fixed synthetic handle so clients that require a non-empty
        // identifier (DuckDB's duckhog, some JDBC drivers) can proceed.
        tracing::debug!("issuing synthetic transaction handle (read-only stub)");
        Ok(ActionBeginTransactionResult {
            transaction_id: Bytes::from_static(NOOP_TRANSACTION_ID),
        })
    }

    #[tracing::instrument(name = "flight_sql.do_action_end_transaction", skip_all, fields(tenant_id, action))]
    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        record_tenant_id(&request);
        // Surface the commit/rollback intent on the span for observability
        // even though we don't act on it.
        tracing::Span::current().record("action", query.action);
        tracing::debug!("accepting end-transaction (read-only stub)");
        Ok(())
    }

    // ─── Delegation ────────────────────────────────────────────────────
    //
    // Every method below forwards directly to the inner service so its
    // real implementation runs. Boilerplate is unavoidable: arrow-flight's
    // `FlightSqlService` is not object-safe, so we cannot use dynamic
    // dispatch, and the trait's default impls return `Unimplemented`
    // (which would shadow the inner service if we did not forward
    // explicitly). Each forwarder is instrumented so the span tree shows
    // per-RPC timing and the tenant identifier.

    #[tracing::instrument(name = "flight_sql.do_handshake", skip_all)]
    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>, Status> {
        // No record_tenant_id: handshake is the only RPC that may not
        // yet carry the tenant header (some clients set it after the
        // handshake completes).
        self.inner.do_handshake(request).await
    }

    #[tracing::instrument(name = "flight_sql.do_get_fallback", skip_all, fields(tenant_id, type_url = %message.type_url))]
    async fn do_get_fallback(&self, request: Request<Ticket>, message: Any) -> Result<Response<DoGetStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_get_fallback(request, message).await
    }

    #[tracing::instrument(
        name = "flight_sql.get_flight_info_statement",
        skip_all,
        fields(tenant_id, sql = %query.query)
    )]
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        record_tenant_id(&request);
        self.inner.get_flight_info_statement(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.get_flight_info_substrait_plan", skip_all, fields(tenant_id))]
    async fn get_flight_info_substrait_plan(
        &self,
        query: CommandStatementSubstraitPlan,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        record_tenant_id(&request);
        self.inner.get_flight_info_substrait_plan(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.get_flight_info_prepared_statement", skip_all, fields(tenant_id))]
    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        record_tenant_id(&request);
        self.inner.get_flight_info_prepared_statement(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.get_flight_info_catalogs", skip_all, fields(tenant_id))]
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        record_tenant_id(&request);
        self.inner.get_flight_info_catalogs(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.get_flight_info_schemas", skip_all, fields(tenant_id))]
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        record_tenant_id(&request);
        self.inner.get_flight_info_schemas(query, request).await
    }

    #[tracing::instrument(
        name = "flight_sql.get_flight_info_tables",
        skip_all,
        fields(tenant_id, include_schema = query.include_schema)
    )]
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        record_tenant_id(&request);
        self.inner.get_flight_info_tables(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.get_flight_info_table_types", skip_all, fields(tenant_id))]
    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        record_tenant_id(&request);
        self.inner.get_flight_info_table_types(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.get_flight_info_sql_info", skip_all, fields(tenant_id))]
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        record_tenant_id(&request);
        self.inner.get_flight_info_sql_info(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.get_flight_info_primary_keys", skip_all, fields(tenant_id))]
    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        record_tenant_id(&request);
        self.inner.get_flight_info_primary_keys(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.get_flight_info_exported_keys", skip_all, fields(tenant_id))]
    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        record_tenant_id(&request);
        self.inner.get_flight_info_exported_keys(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.get_flight_info_imported_keys", skip_all, fields(tenant_id))]
    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        record_tenant_id(&request);
        self.inner.get_flight_info_imported_keys(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.get_flight_info_cross_reference", skip_all, fields(tenant_id))]
    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        record_tenant_id(&request);
        self.inner.get_flight_info_cross_reference(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.get_flight_info_xdbc_type_info", skip_all, fields(tenant_id))]
    async fn get_flight_info_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        record_tenant_id(&request);
        self.inner.get_flight_info_xdbc_type_info(query, request).await
    }

    #[tracing::instrument(
        name = "flight_sql.get_flight_info_fallback",
        skip_all,
        fields(tenant_id, type_url = %cmd.type_url())
    )]
    async fn get_flight_info_fallback(
        &self,
        cmd: Command,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        record_tenant_id(&request);
        self.inner.get_flight_info_fallback(cmd, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_get_statement", skip_all, fields(tenant_id))]
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_get_statement(ticket, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_get_prepared_statement", skip_all, fields(tenant_id))]
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_get_prepared_statement(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_get_catalogs", skip_all, fields(tenant_id))]
    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_get_catalogs(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_get_schemas", skip_all, fields(tenant_id))]
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_get_schemas(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_get_tables", skip_all, fields(tenant_id))]
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_get_tables(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_get_table_types", skip_all, fields(tenant_id))]
    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_get_table_types(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_get_sql_info", skip_all, fields(tenant_id))]
    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_get_sql_info(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_get_primary_keys", skip_all, fields(tenant_id))]
    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_get_primary_keys(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_get_exported_keys", skip_all, fields(tenant_id))]
    async fn do_get_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_get_exported_keys(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_get_imported_keys", skip_all, fields(tenant_id))]
    async fn do_get_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_get_imported_keys(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_get_cross_reference", skip_all, fields(tenant_id))]
    async fn do_get_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_get_cross_reference(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_get_xdbc_type_info", skip_all, fields(tenant_id))]
    async fn do_get_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_get_xdbc_type_info(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_put_fallback", skip_all, fields(tenant_id, type_url = %message.type_url))]
    async fn do_put_fallback(
        &self,
        request: Request<PeekableFlightDataStream>,
        message: Any,
    ) -> Result<Response<DoPutStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_put_fallback(request, message).await
    }

    #[tracing::instrument(name = "flight_sql.do_put_statement_update", skip_all, fields(tenant_id))]
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        record_tenant_id(&request);
        self.inner.do_put_statement_update(ticket, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_put_statement_ingest", skip_all, fields(tenant_id))]
    async fn do_put_statement_ingest(
        &self,
        ticket: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        record_tenant_id(&request);
        self.inner.do_put_statement_ingest(ticket, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_put_prepared_statement_query", skip_all, fields(tenant_id))]
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        record_tenant_id(&request);
        self.inner.do_put_prepared_statement_query(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_put_prepared_statement_update", skip_all, fields(tenant_id))]
    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        record_tenant_id(&request);
        self.inner.do_put_prepared_statement_update(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_put_substrait_plan", skip_all, fields(tenant_id))]
    async fn do_put_substrait_plan(
        &self,
        query: CommandStatementSubstraitPlan,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        record_tenant_id(&request);
        self.inner.do_put_substrait_plan(query, request).await
    }

    #[tracing::instrument(
        name = "flight_sql.do_action_fallback",
        skip_all,
        fields(tenant_id, action_type = %request.get_ref().r#type)
    )]
    async fn do_action_fallback(&self, request: Request<Action>) -> Result<Response<DoActionStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_action_fallback(request).await
    }

    #[tracing::instrument(name = "flight_sql.list_custom_actions", skip_all)]
    async fn list_custom_actions(&self) -> Option<Vec<Result<ActionType, Status>>> {
        self.inner.list_custom_actions().await
    }

    #[tracing::instrument(
        name = "flight_sql.do_action_create_prepared_statement",
        skip_all,
        fields(tenant_id, sql = %query.query)
    )]
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        record_tenant_id(&request);
        self.inner.do_action_create_prepared_statement(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_action_close_prepared_statement", skip_all, fields(tenant_id))]
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        record_tenant_id(&request);
        self.inner.do_action_close_prepared_statement(query, request).await
    }

    #[tracing::instrument(
        name = "flight_sql.do_action_create_prepared_substrait_plan",
        skip_all,
        fields(tenant_id)
    )]
    async fn do_action_create_prepared_substrait_plan(
        &self,
        query: ActionCreatePreparedSubstraitPlanRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        record_tenant_id(&request);
        self.inner.do_action_create_prepared_substrait_plan(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_action_begin_savepoint", skip_all, fields(tenant_id))]
    async fn do_action_begin_savepoint(
        &self,
        query: ActionBeginSavepointRequest,
        request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        record_tenant_id(&request);
        self.inner.do_action_begin_savepoint(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_action_end_savepoint", skip_all, fields(tenant_id))]
    async fn do_action_end_savepoint(
        &self,
        query: ActionEndSavepointRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        record_tenant_id(&request);
        self.inner.do_action_end_savepoint(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_action_cancel_query", skip_all, fields(tenant_id))]
    async fn do_action_cancel_query(
        &self,
        query: ActionCancelQueryRequest,
        request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        record_tenant_id(&request);
        self.inner.do_action_cancel_query(query, request).await
    }

    #[tracing::instrument(name = "flight_sql.do_exchange_fallback", skip_all, fields(tenant_id))]
    async fn do_exchange_fallback(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<Self as FlightService>::DoExchangeStream>, Status> {
        record_tenant_id(&request);
        self.inner.do_exchange_fallback(request).await
    }

    async fn register_sql_info(&self, id: i32, result: &SqlInfo) {
        // Not instrumented: invoked at server startup, not per-request,
        // and has no tenant context.
        self.inner.register_sql_info(id, result).await;
    }
}
