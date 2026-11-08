//! trust's executed statements, each answered twice over the same rows: as the SQL trust sends
//! over Flight SQL, and as its PPL form over `/_plugins/_ppl`. Both answers must equal the rows
//! the test states, derived by hand from the rows it writes.
//!
//! Each statement is rendered with concrete parameters from the trust statement builder named
//! below. Rows of a second tenant are written in the same batch and snapshot, chosen so that a
//! leaked row changes the expected answer, so every case is also a cross-tenant negative.
//!
//! | # | trust statement | test |
//! |---|---|---|
//! | 1 | `logs.rs::logs_list_sql` | `logs::logs_list_first_page_and_continuation_match_their_sql_forms`, `logs::logs_list_with_a_trace_id_filter_matches_its_sql_form` |
//! | 2 | `logs.rs::logs_summary_sql` | `logs::logs_summary_matches_its_sql_form` |
//! | 3 | `logs.rs::logs_find_sql` | `logs::logs_find_matches_its_sql_form` |
//! | 4 | `logs.rs::logs_context_sql` | `logs::logs_context_matches_its_sql_form` |
//! | 5 | `facets.rs::facets_sql` over Logs fields | `logs::log_facets_match_their_sql_forms` |
//! | 6 | `trace.rs::trace_summaries_sql`, list | `traces::trace_list_window_span_filters_and_having_match_their_sql_forms`, `traces::trace_list_with_id_leaves_and_keyset_continuation_matches_its_sql_form` |
//! | 7 | `trace.rs::trace_summaries_sql`, find by trace id | `traces::trace_header_by_id_matches_its_sql_form` |
//! | 8 | `trace.rs::spans_for_trace_sql` | `traces::span_waterfall_over_ten_thousand_rows_matches_with_head_10001` |
//! | 9 | `facets.rs::facets_sql` over Traces fields | `traces::trace_facets_values_of_case_without_else_match_the_sql_form` |
//! | 10 | `operation.rs::operations_by_traces_sql`, one trace | `operations::priced_operations_of_one_trace_match_their_sql_form` |
//! | 11 | `operation.rs::operations_by_traces_sql`, thread | `operations::thread_operations_over_a_trace_id_list_match_their_sql_form` |
//! | 12 | `conversation.rs::list_conversations_sql` | `conversations::conversation_list_matches_its_sql_form`, `conversations::conversation_list_with_a_trace_id_having_matches_its_sql_form` |
//! | 13 | `conversation.rs::summary_conversations_sql` | `conversations::conversation_summary_matches_its_sql_form` |
//! | 14 | `conversation.rs::find_conversation_sql` | `conversations::single_conversation_matches_its_sql_form` |
//! | 15 | `operation.rs::conversation_trace_windows_sql` | `conversations::conversation_trace_windows_match_their_sql_form` |
//! | 16 | `facets.rs::facets_sql` over Conversations fields | `conversations::conversation_facets_match_their_sql_form` |

mod conversations;
mod harness;
mod logs;
mod operations;
mod tables;
mod traces;
