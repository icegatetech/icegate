//! The PPL AST builder: walks the parse tree of the vendored grammar into a [`PplQuery`].
//!
//! The grammar accepts every 3.x query. The builder turns the commands and expressions IceGate
//! plans into the AST and refuses every other one by name, so an unplanned construct is a 400
//! naming it, never a silently different query.

use std::num::IntErrorKind;
use std::rc::Rc;

use antlr4rust::parser_rule_context::ParserRuleContext;
use antlr4rust::rule_context::CustomRuleContext;
use antlr4rust::token::Token;
use antlr4rust::tree::{ParseTree, Tree};
use icegate_query_grammar::ppl::{
    BetweenContextAttrs, BinCommandContextAll, BinCommandContextAttrs, BinSpanValueContextAll,
    BooleanLiteralContextAttrs, BucketNullableArgContextAll, ByClauseContextAttrs, BySpanClauseContextAll,
    BySpanClauseContextAttrs, CaseFunctionCallContextAll, CaseFunctionCallContextAttrs, CommandsContextAll,
    CommandsContextAttrs, CompareExprContextAttrs, ComparisonOperatorContextAll, ComparisonOperatorContextAttrs,
    CountEvalFunctionCallContextAttrs, DataTypeFunctionCallContextAttrs, DedupCommandContextAll,
    DedupCommandContextAttrs, DefaultSortFieldContextAttrs, DescribeCommandContextAttrs,
    DistinctCountFunctionCallContextAttrs, EvalClauseContextAll, EvalClauseContextAttrs, EvalCommandContextAttrs,
    EvalExpressionContextAttrs, EvalFunctionCallContextAttrs, ExpressionContextAll, FieldExprContextAttrs,
    FieldExpressionContextAll, FieldExpressionContextAttrs, FieldListContextAll, FieldListContextAttrs,
    FieldsCommandBodyContextAll, FieldsCommandBodyContextAttrs, FieldsCommandContextAttrs,
    FillNullUsingClauseContextAttrs, FillNullUsingContextAttrs, FillNullValueAllFieldsContextAttrs,
    FillNullValueWithFieldsContextAttrs, FillNullWithClauseContextAttrs, FillNullWithContextAttrs,
    FillnullCommandContextAll, FromClauseContextAll, FromClauseContextAttrs, FunctionArgContextAll,
    FunctionArgContextAttrs, FunctionArgExpressionContextAttrs, FunctionArgsContextAll, FunctionArgsContextAttrs,
    FunctionCallContextAll, FunctionCallContextAttrs, FunctionCallExprContextAttrs, IdentContextAll, IdentContextAttrs,
    IdentsAsQualifiedNameContextAttrs, IdentsAsTableQualifiedNameContextAttrs,
    IdentsAsWildcardQualifiedNameContextAttrs, InExprContextAttrs, InSubqueryExprContext, InSubqueryExprContextAttrs,
    IntegerLiteralContextAll, IsNullPredicateContextAttrs, JoinCommandContextAll, JoinCommandContextAttrs,
    JoinCriteriaContextAttrs, JoinOptionContextAll, LambdaContextAll, LambdaContextAttrs, LambdaExprContextAttrs,
    LiteralValueContextAll, LiteralValueContextAttrs, LiteralValueExprContextAttrs, LogicalExprContextAttrs,
    LogicalExpressionContextAll, LogicalNotContextAttrs, MaxOptionContextAttrs, NestedValueExprContextAttrs,
    NullNotnullContextAttrs, NumericSpanValueContextAttrs, ParseCommandContextAll, ParseCommandContextAttrs,
    PercentileApproxFunctionCallContextAttrs, PercentileShortcutFunctionCallContextAttrs, PplCommandsContextAll,
    PplCommandsContextAttrs, PplStatementContextAttrs, PrefixSortFieldContextAttrs, QualifiedNameContextAll,
    QueryStatementContextAll, QueryStatementContextAttrs, RareTopCommandContextAll, RareTopCommandContextAttrs,
    RenameClauseContextAll, RenameCommandContextAttrs, RenameFieldExpressionContextAll,
    RenameFieldExpressionContextAttrs, ReplacementPairContextAttrs, RootContextAttrs, SearchCommandContextAll,
    SearchFromContextAttrs, SelectFieldExpressionContextAll, SelectFieldExpressionContextAttrs,
    SortCommandContextAttrs, SortFieldContextAll, SortFieldExpressionContextAll, SortFieldExpressionContextAttrs,
    SortbyClauseContextAttrs, SpanClauseContextAttrs, SpanLiteralContextAll, SpanLiteralContextAttrs,
    StatsAggTermContextAll, StatsAggTermContextAttrs, StatsArgsContextAttrs, StatsByClauseContextAll,
    StatsByClauseContextAttrs, StatsCommandContextAll, StatsCommandContextAttrs, StatsFunctionCallContextAttrs,
    StatsFunctionContextAll, StringLiteralContextAll, SubSearchContextAll, SubSearchContextAttrs,
    SuffixSortFieldContextAttrs, TableCommandContextAttrs, TableIdentContextAll, TableIdentContextAttrs,
    TableOrSubqueryClauseContextAll, TableOrSubqueryClauseContextAttrs, TableQualifiedNameContextAll,
    TableSourceClauseContextAll, TableSourceClauseContextAttrs, TableSourceContextAll, TableSourceContextAttrs,
    TimechartCommandContextAll, TimechartCommandContextAttrs, TimechartParameterContextAttrs,
    TimestampFunctionCallContextAttrs, TypeOptionContextAttrs, ValueExprContextAttrs, ValueExpressionContextAll,
    ValueListContextAttrs, ValuesAggFunctionCallContextAttrs, ValuesAggFunctionContextAttrs,
    WcFieldExpressionContextAttrs, WcFieldListContextAttrs, WcQualifiedNameContextAll, WhereCommandContextAttrs,
    WildcardContextAll, WildcardContextAttrs, parse_ppl_root, ruleNames,
};

use super::ast::{
    ArithmeticOperator, BinCommand, BinSpan, CastTarget, CompareOperator, DedupCommand, FillnullCommand,
    FillnullReplacements, JoinCommand, JoinKind, ParseCommand, PplCommand, PplExpr, PplGroupKey, PplLiteral, PplQuery,
    PplSortKey, PplSource, RankCommand, SpanUnit, StatsCommand, TimechartCommand,
};
use super::error::PplError;
use super::plan::RESERVED_NAME_PREFIX;

/// How many rows `head` keeps when it names no number.
const DEFAULT_HEAD_COUNT: usize = 10;

/// How many value combinations `top` and `rare` keep per group when they name no number.
const DEFAULT_RANK_COUNT: usize = 10;

/// How many series `timechart` keeps before grouping the rest as `OTHER`.
const DEFAULT_TIMECHART_LIMIT: usize = 10;

/// The span length of a `timechart` that sets no `span`, counted in
/// [`DEFAULT_TIMECHART_SPAN_UNIT`].
const DEFAULT_TIMECHART_SPAN_LENGTH: f64 = 1.0;

/// The span unit of a `timechart` that sets no `span`.
const DEFAULT_TIMECHART_SPAN_UNIT: SpanUnit = SpanUnit::Minute;

/// How many rows of each field combination `dedup` keeps when it names no number.
const DEFAULT_DEDUP_COUNT: usize = 1;

/// The greatest time span length a span key holds: the AST carries a span length as an `f64`,
/// which holds every whole number up to this one exactly, so a longer length could bucket by a
/// width other than the one written.
const MAX_EXACT_SPAN_LENGTH: i64 = 1 << f64::MANTISSA_DIGITS;

/// The field a `span` or `timechart` buckets when it names none.
const IMPLICIT_TIME_FIELD: &str = "@timestamp";

/// The count column of `top` and `rare` when `countfield` names none.
const DEFAULT_COUNT_FIELD: &str = "count";

/// `bucket_nullable` when a command sets none: rows with a NULL group key form a group of their own.
const DEFAULT_BUCKET_NULLABLE: bool = true;

/// Parse `text` into a [`PplQuery`].
///
/// `search source=…`, `source=…` and `index=…` build the same query. Keywords match in any case;
/// field and index names keep the case they are written in.
///
/// # Errors
///
/// Returns [`PplError::Syntax`] for text the grammar rejects and for a query without a source,
/// [`PplError::UnsupportedCommand`] / [`PplError::UnsupportedFunction`] naming a command or
/// function IceGate does not plan, and [`PplError::Semantic`] for a construct the grammar accepts
/// but whose meaning IceGate refuses (a negative `head`, a sort key with both a sign and a
/// direction, a field name with the reserved prefix).
pub(crate) fn parse_ppl(text: &str) -> Result<PplQuery, PplError> {
    let root = parse_ppl_root(text)?;
    let Some(statement) = root.pplStatement() else {
        let (line, column) = root
            .EOF()
            .map_or((1, 0), |end| (end.symbol.get_line(), end.symbol.get_column()));
        return Err(PplError::Syntax {
            offending_text: "<EOF>".to_string(),
            line,
            column,
        });
    };
    if statement.explainStatement().is_some() {
        return Err(build_unsupported_command_error("explain"));
    }
    let query = statement.queryStatement().ok_or_else(|| PplError::Internal {
        reason: "a PPL statement without a query".to_string(),
    })?;
    build_query(&query)
}

fn build_query(query: &QueryStatementContextAll<'_>) -> Result<PplQuery, PplError> {
    let first_command = query.pplCommands().ok_or_else(|| PplError::Internal {
        reason: "a PPL query without a first command".to_string(),
    })?;
    let source = build_source(&first_command)?;
    let commands = query
        .commands_all()
        .iter()
        .map(|command| build_command(command))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PplQuery { source, commands })
}

fn build_source(first_command: &PplCommandsContextAll<'_>) -> Result<PplSource, PplError> {
    if let Some(describe) = first_command.describeCommand() {
        let clause = describe
            .tableSourceClause()
            .ok_or_else(|| build_missing_part_error("describe"))?;
        return Ok(PplSource::Describe {
            index: name_table_sources(&clause)?,
        });
    }
    let Some(search) = first_command.searchCommand() else {
        return Err(build_unsupported_command_error(&name_first_child_rule(first_command)));
    };
    build_search_source(&search)
}

/// The source a search command names (`search source=…`, `source=…`, `index=…`).
fn build_search_source(search: &SearchCommandContextAll<'_>) -> Result<PplSource, PplError> {
    let SearchCommandContextAll::SearchFromContext(search) = search else {
        return Err(PplError::Internal {
            reason: "an unlabeled search command".to_string(),
        });
    };
    if !search.searchExpression_all().is_empty() {
        return Err(build_unsupported_command_error("search terms"));
    }
    let from = search.fromClause().ok_or_else(|| PplError::Internal {
        reason: "a search command without a from clause".to_string(),
    })?;
    build_index_source(&from)
}

fn build_index_source(from: &FromClauseContextAll<'_>) -> Result<PplSource, PplError> {
    let Some(clause) = from.tableOrSubqueryClause() else {
        let kind = if from.tableFunction().is_some() {
            "source=<table function>"
        } else {
            "source=[<sources>]"
        };
        return Err(build_unsupported_command_error(kind));
    };
    build_source_clause(&clause)
}

/// Builds the source a `source=` clause names: an index expression, or a bracketed subsearch with its
/// optional `as` alias.
///
/// # Errors
///
/// `Semantic` for an alias on an index source (`source=logs as l`): no command reads it, and a join names
/// its sides with `left=` / `right=`; `Semantic` for an alias with the reserved prefix. The subsearch's
/// own errors.
fn build_source_clause(clause: &TableOrSubqueryClauseContextAll<'_>) -> Result<PplSource, PplError> {
    let is_subsearch = clause.subSearch().is_some();
    let (query, alias) = build_table_or_subquery(clause)?;
    if is_subsearch {
        return Ok(PplSource::Subsearch {
            query: Box::new(query),
            alias,
        });
    }
    if let (Some(alias), PplSource::Index { expression }) = (&alias, &query.source) {
        return Err(PplError::Semantic {
            reason: format!(
                "`source={expression} as {alias}`: an alias on an index source is not supported; \
                 name a join's sides with left=<alias> and right=<alias>"
            ),
        });
    }
    Ok(query.source)
}

/// The index expression a `tableSourceClause` names: its table sources, comma-separated.
fn name_table_sources(clause: &TableSourceClauseContextAll<'_>) -> Result<String, PplError> {
    let names = clause
        .tableSource_all()
        .iter()
        .map(|source| name_table_source(source))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(names.join(","))
}

fn name_table_source(source: &TableSourceContextAll<'_>) -> Result<String, PplError> {
    if let Some(date_suffixed_name) = source.ID_DATE_SUFFIX() {
        return Ok(date_suffixed_name.get_text());
    }
    let table_name = source.tableQualifiedName().ok_or_else(|| PplError::Internal {
        reason: "a table source without a name".to_string(),
    })?;
    let TableQualifiedNameContextAll::IdentsAsTableQualifiedNameContext(table_name) = table_name.as_ref() else {
        return Err(PplError::Internal {
            reason: "an unlabeled table name".to_string(),
        });
    };
    let head = table_name.tableIdent().ok_or_else(|| PplError::Internal {
        reason: "a table name without an identifier".to_string(),
    })?;
    let mut parts = vec![name_table_ident(&head)?];
    for ident in table_name.ident_all() {
        parts.push(unquote_identifier(&ident));
    }
    Ok(parts.join("."))
}

fn name_table_ident(table_ident: &TableIdentContextAll<'_>) -> Result<String, PplError> {
    let ident = table_ident.ident().ok_or_else(|| PplError::Internal {
        reason: "a table identifier without a name".to_string(),
    })?;
    let cluster = table_ident.CLUSTER().map(|prefix| prefix.get_text()).unwrap_or_default();
    Ok(format!("{cluster}{}", unquote_identifier(&ident)))
}

fn build_command(command: &CommandsContextAll<'_>) -> Result<PplCommand, PplError> {
    if let Some(filter) = command.whereCommand() {
        let predicate = filter.logicalExpression().ok_or_else(|| build_missing_part_error("where"))?;
        return Ok(PplCommand::Where(build_logical_expression(&predicate)?));
    }
    if let Some(fields) = command.fieldsCommand() {
        let body = fields.fieldsCommandBody().ok_or_else(|| build_missing_part_error("fields"))?;
        return build_fields(&body);
    }
    if let Some(table) = command.tableCommand() {
        let body = table.fieldsCommandBody().ok_or_else(|| build_missing_part_error("table"))?;
        return build_fields(&body);
    }
    if let Some(sort) = command.sortCommand() {
        if sort.count.is_some() {
            return Err(PplError::Semantic {
                reason: "sort with a row count is not supported; append | head N instead".to_string(),
            });
        }
        let clause = sort.sortbyClause().ok_or_else(|| build_missing_part_error("sort"))?;
        let keys = clause
            .sortField_all()
            .iter()
            .map(|field| build_sort_key(field))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(PplCommand::Sort(keys));
    }
    if let Some(head) = command.headCommand() {
        let count = head
            .number
            .as_ref()
            .map(|number| read_count(number, "head"))
            .transpose()?
            .unwrap_or(DEFAULT_HEAD_COUNT);
        let offset = head
            .from
            .as_ref()
            .map(|from| read_count(from, "head ... from"))
            .transpose()?
            .unwrap_or(0);
        return Ok(PplCommand::Head { count, offset });
    }
    if let Some(eval) = command.evalCommand() {
        let assignments = eval
            .evalClause_all()
            .iter()
            .map(|clause| build_assignment(clause))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(PplCommand::Eval(assignments));
    }
    if let Some(rename) = command.renameCommand() {
        let renames = rename
            .renameClause_all()
            .iter()
            .map(|clause| build_rename(clause))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(PplCommand::Rename(renames));
    }
    if let Some(stats) = command.statsCommand() {
        return Ok(PplCommand::Stats(build_stats(&stats)?));
    }
    if let Some(rank) = command.rareTopCommand() {
        let is_top = rank.TOP().is_some();
        let rank = build_rank(&rank, if is_top { "top" } else { "rare" })?;
        return Ok(if is_top {
            PplCommand::Top(rank)
        } else {
            PplCommand::Rare(rank)
        });
    }
    if let Some(dedup) = command.dedupCommand() {
        return Ok(PplCommand::Dedup(build_dedup(&dedup)?));
    }
    if let Some(timechart) = command.timechartCommand() {
        return Ok(PplCommand::Timechart(build_timechart(&timechart)?));
    }
    if let Some(bin) = command.binCommand() {
        return Ok(PplCommand::Bin(build_bin(&bin)?));
    }
    if let Some(parse) = command.parseCommand() {
        return build_parse(&parse);
    }
    if let Some(fillnull) = command.fillnullCommand() {
        return build_fillnull(&fillnull);
    }
    if let Some(join) = command.joinCommand() {
        return build_join_command(&join);
    }
    Err(build_unsupported_command_error(&name_first_child_rule(command)))
}

/// The join type IceGate plans for a written join type; no type at all means inner.
///
/// # Errors
///
/// `Semantic` naming every other type (`right`, `full`, `cross`, `semi`, `anti` and their `left`
/// spellings): IceGate plans inner and left joins only.
fn build_join_kind(text: &str) -> Result<JoinKind, PplError> {
    match text.to_ascii_lowercase().as_str() {
        "inner" => Ok(JoinKind::Inner),
        "left" | "left outer" | "outer" => Ok(JoinKind::Left),
        other => Err(PplError::Semantic {
            reason: format!("join type `{other}` is not supported; IceGate plans inner and left joins"),
        }),
    }
}

/// The tokens of `context` as the query wrote them, one space apart: `get_text` joins them with
/// none, so `left outer` would read `leftouter`, a word the query never wrote.
fn spell_tokens<'input, T: Tree<'input> + ?Sized>(context: &T) -> String {
    context
        .get_children()
        .map(|child| child.get_text())
        .collect::<Vec<_>>()
        .join(" ")
}

/// The name an alias declares, unquoted like a field name; a reserved name is refused
/// ([`check_field_name`]).
fn name_alias(name: &QualifiedNameContextAll<'_>) -> Result<String, PplError> {
    join_qualified_name(name).and_then(check_field_name)
}

/// Builds a bracketed subsearch: its search command and the commands piped after it.
fn build_subsearch(subsearch: &SubSearchContextAll<'_>) -> Result<PplQuery, PplError> {
    let search = subsearch.searchCommand().ok_or_else(|| build_missing_part_error("subsearch"))?;
    let commands = subsearch
        .commands_all()
        .iter()
        .map(|command| build_command(command))
        .collect::<Result<Vec<_>, PplError>>()?;
    Ok(PplQuery {
        source: build_search_source(&search)?,
        commands,
    })
}

/// Builds the side a `tableOrSubqueryClause` names, an index expression or a bracketed subsearch, with
/// the alias written after it.
fn build_table_or_subquery(
    clause: &TableOrSubqueryClauseContextAll<'_>,
) -> Result<(PplQuery, Option<String>), PplError> {
    if let Some(subsearch) = clause.subSearch() {
        let alias = clause.alias.as_deref().map(name_alias).transpose()?;
        return Ok((build_subsearch(&subsearch)?, alias));
    }
    let sources = clause.tableSourceClause().ok_or_else(|| build_missing_part_error("source"))?;
    let query = PplQuery {
        source: PplSource::Index {
            expression: name_table_sources(&sources)?,
        },
        commands: Vec::new(),
    };
    Ok((query, sources.alias.as_deref().map(name_alias).transpose()?))
}

/// Builds `join` in its criteria form.
///
/// # Errors
///
/// `Semantic` for a join type other than inner or left, two disagreeing types, a `max` that is not a
/// non-negative integer, join hints, the field-list form, which has no criteria, or an alias with the
/// reserved prefix.
fn build_join_command(join: &JoinCommandContextAll<'_>) -> Result<PplCommand, PplError> {
    let criteria = join.joinCriteria().ok_or_else(|| PplError::Semantic {
        reason: "join needs criteria: write `join left=<l> right=<r> on <l>.<field> = <r>.<field> <right side>`"
            .to_string(),
    })?;
    if join.joinHintList().is_some() {
        return Err(PplError::Semantic {
            reason: "join hints are not supported".to_string(),
        });
    }
    let prefix = join
        .sqlLikeJoinType()
        .map(|join_type| {
            let text = spell_tokens(&*join_type);
            build_join_kind(&text).map(|kind| (text, kind))
        })
        .transpose()?;
    // `Option` doubles as the "already written" flag: a second `type=` or `max=` would otherwise
    // silently replace the first, so a query that wrote `max=1` could be planned as unlimited.
    let mut option_kind = None;
    let mut max_matches = None;
    for option in join.joinOption_all() {
        match option.as_ref() {
            JoinOptionContextAll::TypeOptionContext(type_option) => {
                if option_kind.is_some() {
                    return Err(build_repeated_join_option_error("type"));
                }
                let text = spell_tokens(&*type_option.joinType().ok_or_else(|| build_missing_part_error("join"))?);
                let written_kind = build_join_kind(&text)?;
                if let Some((prefix_text, prefix_kind)) = &prefix
                    && *prefix_kind != written_kind
                {
                    return Err(PplError::Semantic {
                        reason: format!("join type={text} contradicts `{prefix_text}` written before `join`"),
                    });
                }
                option_kind = Some(written_kind);
            }
            JoinOptionContextAll::MaxOptionContext(max_option) => {
                if max_matches.is_some() {
                    return Err(build_repeated_join_option_error("max"));
                }
                let text = max_option
                    .integerLiteral()
                    .ok_or_else(|| build_missing_part_error("join"))?
                    .get_text();
                let limit = text.parse::<usize>().map_err(|error| PplError::Semantic {
                    reason: match error.kind() {
                        IntErrorKind::PosOverflow => format!("join max={text} is out of range"),
                        _ => format!("join max={text}: max must be a non-negative integer"),
                    },
                })?;
                max_matches = Some(limit);
            }
            // The criteria form renames a colliding right column, so `overwrite=` has nothing to decide
            // and is accepted unchanged.
            JoinOptionContextAll::OverwriteOptionContext(_) => {}
            JoinOptionContextAll::Error(_) => return Err(build_missing_part_error("join")),
        }
    }
    let side = join.sideAlias();
    let left_alias = side
        .as_ref()
        .and_then(|side| side.leftAlias.as_deref())
        .map(name_alias)
        .transpose()?;
    let written_right_alias = side
        .as_ref()
        .and_then(|side| side.rightAlias.as_deref())
        .map(name_alias)
        .transpose()?;
    let right_side = join.tableOrSubqueryClause().ok_or_else(|| build_missing_part_error("join"))?;
    let (right, right_side_alias) = build_table_or_subquery(&right_side)?;
    let predicate = criteria.logicalExpression().ok_or_else(|| build_missing_part_error("join"))?;
    Ok(PplCommand::Join(JoinCommand {
        kind: option_kind.or_else(|| prefix.map(|(_, kind)| kind)).unwrap_or(JoinKind::Inner),
        left_alias,
        right_alias: written_right_alias.or(right_side_alias),
        criteria: build_logical_expression(&predicate)?,
        max_matches: max_matches.unwrap_or(0),
        right: Box::new(right),
    }))
}

/// Builds `parse <expression> '<pattern>'`.
fn build_parse(parse: &ParseCommandContextAll<'_>) -> Result<PplCommand, PplError> {
    let source = parse.expression().ok_or_else(|| build_missing_part_error("parse"))?;
    let pattern = parse.stringLiteral().ok_or_else(|| build_missing_part_error("parse"))?;
    Ok(PplCommand::Parse(ParseCommand {
        source: build_expression(&source)?,
        pattern: unquote_string(&pattern),
    }))
}

/// Builds the four `fillnull` forms; a form without a field list fills every output column.
fn build_fillnull(fillnull: &FillnullCommandContextAll<'_>) -> Result<PplCommand, PplError> {
    let replacements = match fillnull {
        FillnullCommandContextAll::FillNullWithClauseContext(clause) => {
            let with_clause = clause.fillNullWith().ok_or_else(|| build_missing_part_error("fillnull"))?;
            let replacement = build_replacement(with_clause.valueExpression())?;
            match with_clause.fieldList() {
                Some(fields) => FillnullReplacements::Fields(pair_fields_with(&fields, &replacement)?),
                None => FillnullReplacements::AllFields(replacement),
            }
        }
        FillnullCommandContextAll::FillNullUsingClauseContext(clause) => {
            let using_clause = clause.fillNullUsing().ok_or_else(|| build_missing_part_error("fillnull"))?;
            let pairs = using_clause
                .replacementPair_all()
                .iter()
                .map(|pair| {
                    let field = pair.fieldExpression().ok_or_else(|| build_missing_part_error("fillnull"))?;
                    Ok((name_field(&field)?, build_replacement(pair.valueExpression())?))
                })
                .collect::<Result<Vec<_>, PplError>>()?;
            FillnullReplacements::Fields(pairs)
        }
        FillnullCommandContextAll::FillNullValueWithFieldsContext(clause) => {
            let replacement = build_replacement(clause.valueExpression())?;
            let fields = clause.fieldList().ok_or_else(|| build_missing_part_error("fillnull"))?;
            FillnullReplacements::Fields(pair_fields_with(&fields, &replacement)?)
        }
        FillnullCommandContextAll::FillNullValueAllFieldsContext(clause) => {
            FillnullReplacements::AllFields(build_replacement(clause.valueExpression())?)
        }
        FillnullCommandContextAll::Error(_) => return Err(build_missing_part_error("fillnull")),
    };
    Ok(PplCommand::Fillnull(FillnullCommand { replacements }))
}

/// Builds a `fillnull` replacement value.
fn build_replacement(value: Option<Rc<ValueExpressionContextAll<'_>>>) -> Result<PplExpr, PplError> {
    let value = value.ok_or_else(|| build_missing_part_error("fillnull"))?;
    build_value(&value)
}

/// Pairs every field of a `fillnull` field list with one replacement value.
fn pair_fields_with(
    fields: &FieldListContextAll<'_>,
    replacement: &PplExpr,
) -> Result<Vec<(String, PplExpr)>, PplError> {
    fields
        .fieldExpression_all()
        .iter()
        .map(|field| Ok((name_field(field)?, replacement.clone())))
        .collect()
}

/// A `stats` command; its `partitions`, `allnum`, `delim` and `dedup_splitvalues` options are
/// refused, and the last `bucket_nullable` written wins.
fn build_stats(stats: &StatsCommandContextAll<'_>) -> Result<StatsCommand, PplError> {
    if stats.dedupSplitArg().is_some() {
        return Err(build_unsupported_option_error("stats", "dedup_splitvalues"));
    }
    let mut is_bucket_nullable = DEFAULT_BUCKET_NULLABLE;
    if let Some(arguments) = stats.statsArgs() {
        if !arguments.partitionsArg_all().is_empty() {
            return Err(build_unsupported_option_error("stats", "partitions"));
        }
        if !arguments.allnumArg_all().is_empty() {
            return Err(build_unsupported_option_error("stats", "allnum"));
        }
        if !arguments.delimArg_all().is_empty() {
            return Err(build_unsupported_option_error("stats", "delim"));
        }
        for argument in arguments.bucketNullableArg_all() {
            is_bucket_nullable = read_bucket_nullable(&argument)?;
        }
    }
    let aggregations = stats
        .statsAggTerm_all()
        .iter()
        .map(|term| build_aggregation(term))
        .collect::<Result<Vec<_>, _>>()?;
    let by = match stats.statsByClause() {
        Some(clause) => build_group_keys(&clause)?,
        None => Vec::new(),
    };
    Ok(StatsCommand {
        aggregations,
        by,
        is_bucket_nullable,
    })
}

/// The value of a `bucket_nullable=<bool>` argument.
fn read_bucket_nullable(argument: &BucketNullableArgContextAll<'_>) -> Result<bool, PplError> {
    let value = argument
        .bucket_nullable
        .as_deref()
        .ok_or_else(|| build_missing_part_error("bucket_nullable"))?;
    Ok(value.TRUE().is_some())
}

/// One aggregation with its output name: the alias, or the call's text without whitespace.
fn build_aggregation(term: &StatsAggTermContextAll<'_>) -> Result<(String, PplExpr), PplError> {
    let function = term.statsFunction().ok_or_else(|| build_missing_part_error("aggregation"))?;
    let name = match term.alias.as_deref() {
        Some(alias) => {
            let alias_name = alias.wcQualifiedName().ok_or_else(|| build_missing_part_error("alias"))?;
            name_wildcard_path(&alias_name)?
        }
        None => function.get_text(),
    };
    Ok((name, build_aggregate_call(&function)?))
}

/// An aggregate call as a [`PplExpr::FunctionCall`]: `count()` without arguments,
/// `count(eval(<predicate>))` with an `eval` call argument, `percentile(x, p)` for every
/// percentile spelling (`p95(x)`, `perc99.5(x)`, `percentile_approx(x, 95)`, `median(x)`), and
/// `dc(x)` for every distinct count spelling. The planner decides which calls it plans.
fn build_aggregate_call(function: &StatsFunctionContextAll<'_>) -> Result<PplExpr, PplError> {
    let call = |name: &str, arguments: Vec<PplExpr>| PplExpr::FunctionCall {
        name: name.to_string(),
        arguments,
    };
    match function {
        StatsFunctionContextAll::CountAllFunctionCallContext(_) => Ok(call("count", Vec::new())),
        StatsFunctionContextAll::CountEvalFunctionCallContext(count) => {
            let predicate = count
                .evalExpression()
                .and_then(|eval| eval.logicalExpression())
                .ok_or_else(|| build_missing_part_error("count(eval())"))?;
            Ok(call(
                "count",
                vec![call("eval", vec![build_logical_expression(&predicate)?])],
            ))
        }
        StatsFunctionContextAll::PercentileShortcutFunctionCallContext(shortcut) => {
            let token = shortcut
                .PERCENTILE_SHORTCUT()
                .ok_or_else(|| build_missing_part_error("percentile"))?
                .get_text();
            let digits = token.trim_start_matches(|character: char| character.is_ascii_alphabetic());
            let percent = digits.parse::<f64>().map_err(|_| PplError::Semantic {
                reason: format!("[{token}] names no percentile"),
            })?;
            let value = shortcut
                .valueExpression()
                .ok_or_else(|| build_missing_part_error("percentile"))?;
            Ok(call(
                "percentile",
                vec![build_value(&value)?, PplExpr::Literal(PplLiteral::Double(percent))],
            ))
        }
        StatsFunctionContextAll::DistinctCountFunctionCallContext(distinct) => {
            let value = distinct.valueExpression().ok_or_else(|| build_missing_part_error("dc"))?;
            Ok(call("dc", vec![build_value(&value)?]))
        }
        StatsFunctionContextAll::ValuesAggFunctionCallContext(values) => {
            let value = values
                .valuesAggFunction()
                .and_then(|function| function.valueExpression())
                .ok_or_else(|| build_missing_part_error("values"))?;
            Ok(call("values", vec![build_value(&value)?]))
        }
        StatsFunctionContextAll::PercentileApproxFunctionCallContext(percentile) => {
            let function = percentile
                .percentileApproxFunction()
                .ok_or_else(|| build_missing_part_error("percentile"))?;
            let value = function
                .aggField
                .as_deref()
                .ok_or_else(|| build_missing_part_error("percentile"))?;
            let percent = function
                .percent
                .as_deref()
                .ok_or_else(|| build_missing_part_error("percentile"))?;
            let percent = percent.get_text().parse::<f64>().map_err(|_| PplError::Semantic {
                reason: format!("[{}] is not a percentile", percent.get_text()),
            })?;
            Ok(call(
                "percentile",
                vec![build_value(value)?, PplExpr::Literal(PplLiteral::Double(percent))],
            ))
        }
        StatsFunctionContextAll::StatsFunctionCallContext(named) => {
            let name = named
                .statsFunctionName()
                .ok_or_else(|| build_missing_part_error("aggregation"))?
                .get_text()
                .to_ascii_lowercase();
            let arguments = match named.functionArgs() {
                Some(arguments) => build_arguments(&arguments)?,
                None => Vec::new(),
            };
            if name == "median" {
                let mut arguments = arguments;
                arguments.push(PplExpr::Literal(PplLiteral::Double(50.0)));
                return Ok(call("percentile", arguments));
            }
            Ok(call(&name, arguments))
        }
        StatsFunctionContextAll::TakeAggFunctionCallContext(_) => Err(build_unsupported_function_error("take")),
        StatsFunctionContextAll::PerFunctionCallContext(per) => Err(refuse_function_call(per)),
        StatsFunctionContextAll::Error(_) => Err(PplError::Internal {
            reason: "an unlabeled aggregation".to_string(),
        }),
    }
}

/// The group keys of a `by` clause: the span key first, then the fields in the order written.
fn build_group_keys(clause: &StatsByClauseContextAll<'_>) -> Result<Vec<PplGroupKey>, PplError> {
    let mut keys = Vec::new();
    if let Some(span) = clause.bySpanClause() {
        keys.push(build_span_key(&span)?);
    }
    if let Some(fields) = clause.fieldList() {
        for field in fields.fieldExpression_all() {
            keys.push(PplGroupKey::Field(name_field(&field)?));
        }
    }
    Ok(keys)
}

/// The `span(…) [as <alias>]` key; a clause without a field buckets the implicit time field.
fn build_span_key(clause: &BySpanClauseContextAll<'_>) -> Result<PplGroupKey, PplError> {
    let span = clause.spanClause().ok_or_else(|| build_missing_part_error("span"))?;
    let field = match span.fieldExpression() {
        Some(field) => name_field(&field)?,
        None => IMPLICIT_TIME_FIELD.to_string(),
    };
    let value = span.value.as_deref().ok_or_else(|| build_missing_part_error("span"))?;
    let (length, unit) = read_span_literal(value)?;
    let alias = clause
        .alias
        .as_deref()
        .map(|alias| join_qualified_name(alias).and_then(check_field_name))
        .transpose()?;
    Ok(PplGroupKey::Span {
        field,
        length,
        unit,
        alias,
    })
}

/// A span or bin length as written, before its unit spelling is resolved.
enum SpanText {
    /// A bare number above 0.
    Numeric(f64),
    /// A positive whole number of the unit spelled `unit_text`.
    Time {
        /// The number of units.
        length: i64,
        /// The unit as written.
        unit_text: String,
    },
}

/// Split a span or bin literal into its length and its unit spelling.
///
/// # Errors
///
/// Returns [`PplError::Semantic`] for a bare number that is not a finite number above 0, a
/// fractional length with a unit (`1.5h`, `1.5d`), a zero length, and a length beyond `i64`.
/// Whether the width of a length in its unit fits is checked where the width is computed.
fn split_span_text(literal: &SpanLiteralContextAll<'_>) -> Result<SpanText, PplError> {
    let text = literal.get_text();
    if literal.INTEGER_LITERAL().is_some() || literal.DECIMAL_LITERAL().is_some() {
        let length = text.parse::<f64>().map_err(|_| build_invalid_span_error(&text))?;
        if length <= 0.0 || !length.is_finite() {
            return Err(build_invalid_span_error(&text));
        }
        return Ok(SpanText::Numeric(length));
    }
    if literal.SPANLENGTH().is_none() {
        return Err(PplError::Semantic {
            reason: format!("span length [{text}] must be a whole number of units"),
        });
    }
    let unit_start = text.find(|character: char| !character.is_ascii_digit()).unwrap_or(text.len());
    let (length_digits, unit_text) = text.split_at(unit_start);
    // The lexer's SPANLENGTH starts with at least one digit, so only an overflow fails here.
    let length = length_digits.parse::<i64>().map_err(|_| build_out_of_range_span_error(&text))?;
    if length == 0 {
        return Err(build_invalid_span_error(&text));
    }
    Ok(SpanText::Time {
        length,
        unit_text: unit_text.to_string(),
    })
}

/// A span length and its unit: a bare number is a numeric span; `<digits><unit>` a time span of
/// [`parse_span_unit`], whose length is at most [`MAX_EXACT_SPAN_LENGTH`].
fn read_span_literal(literal: &SpanLiteralContextAll<'_>) -> Result<(f64, SpanUnit), PplError> {
    Ok(match split_span_text(literal)? {
        SpanText::Numeric(length) => (length, SpanUnit::Numeric),
        SpanText::Time { length, unit_text } => {
            if length > MAX_EXACT_SPAN_LENGTH {
                return Err(build_out_of_range_span_error(&literal.get_text()));
            }
            let unit = parse_span_unit(&unit_text)?;
            // Exact: the length is at most `MAX_EXACT_SPAN_LENGTH`.
            #[allow(clippy::cast_precision_loss)]
            let length = length as f64;
            (length, unit)
        }
    })
}

/// The unit a `span` or `timechart` length names. `M` (months) and `m` (minutes) match
/// case-sensitively and every other spelling in any case; the spellings the lexer admits in a
/// span length but no span buckets by are refused. The `Spans` section of `ppl/README.md` lists
/// the spellings.
///
/// # Errors
///
/// Returns [`PplError::Semantic`] naming a spelling outside that list.
pub(crate) fn parse_span_unit(text: &str) -> Result<SpanUnit, PplError> {
    match text {
        "M" => return Ok(SpanUnit::Month),
        "m" => return Ok(SpanUnit::Minute),
        _ => {}
    }
    let unit = match text.to_ascii_lowercase().as_str() {
        "ms" | "millisecond" => SpanUnit::Millisecond,
        "s" | "sec" | "secs" | "second" | "seconds" => SpanUnit::Second,
        "min" | "mins" | "minute" | "minutes" => SpanUnit::Minute,
        "h" | "hr" | "hrs" | "hour" | "hours" => SpanUnit::Hour,
        "d" | "day" | "days" => SpanUnit::Day,
        "w" | "week" | "weeks" => SpanUnit::Week,
        "mon" | "month" | "months" => SpanUnit::Month,
        "q" | "qtr" | "qtrs" | "quarter" | "quarters" => SpanUnit::Quarter,
        "y" | "year" | "years" => SpanUnit::Year,
        _ => {
            return Err(PplError::Semantic {
                reason: format!("[{text}] is not a span unit"),
            });
        }
    };
    Ok(unit)
}

/// The unit a `bin` span names. `M` (months), `m` (minutes), `us`, `cs` and `ds` match
/// case-sensitively and every other spelling in any case; week, quarter and year spellings do not
/// bin and are refused. The `Bin` section of `ppl/README.md` lists the spellings.
///
/// # Errors
///
/// Returns [`PplError::Semantic`] naming a spelling outside that list.
pub(crate) fn parse_bin_unit(text: &str) -> Result<SpanUnit, PplError> {
    match text {
        "M" => return Ok(SpanUnit::Month),
        "m" => return Ok(SpanUnit::Minute),
        "us" => return Ok(SpanUnit::Microsecond),
        "cs" => return Ok(SpanUnit::Centisecond),
        "ds" => return Ok(SpanUnit::Decisecond),
        _ => {}
    }
    let unit = match text.to_ascii_lowercase().as_str() {
        "ms" => SpanUnit::Millisecond,
        "s" | "sec" | "secs" | "second" | "seconds" => SpanUnit::Second,
        "min" | "mins" | "minute" | "minutes" => SpanUnit::Minute,
        "h" | "hr" | "hrs" | "hour" | "hours" => SpanUnit::Hour,
        "d" | "day" | "days" => SpanUnit::Day,
        "mon" | "month" | "months" => SpanUnit::Month,
        _ => {
            return Err(PplError::Semantic {
                reason: format!("[{text}] is not a bin span unit"),
            });
        }
    };
    Ok(unit)
}

/// A `top` or `rare` command, `command` naming which in its refusals; a count below 1 and
/// `showperc` / `percentfield` are refused.
fn build_rank(rank: &RareTopCommandContextAll<'_>, command: &str) -> Result<RankCommand, PplError> {
    let count = rank
        .number
        .as_deref()
        .map(|number| read_positive_count(number, command))
        .transpose()?
        .unwrap_or(DEFAULT_RANK_COUNT);
    let mut rank_command = RankCommand {
        count,
        fields: build_field_list(rank.fieldList().as_deref())?,
        by: build_field_list(rank.byClause().and_then(|by| by.fieldList()).as_deref())?,
        count_field: DEFAULT_COUNT_FIELD.to_string(),
        is_count_shown: true,
        is_null_kept: true,
    };
    for option in rank.rareTopOption_all() {
        if let Some(count_field) = option.countField.as_deref() {
            rank_command.count_field = check_field_name(unquote_string(count_field))?;
        }
        if let Some(shows_count) = option.showCount.as_deref() {
            rank_command.is_count_shown = shows_count.TRUE().is_some();
        }
        if let Some(uses_null) = option.useNull.as_deref() {
            rank_command.is_null_kept = uses_null.TRUE().is_some();
        }
        if option.showPerc.is_some() {
            return Err(build_unsupported_option_error(command, "showperc"));
        }
        if option.percentField.is_some() {
            return Err(build_unsupported_option_error(command, "percentfield"));
        }
    }
    Ok(rank_command)
}

/// The field names of a field list, in order; an absent list is empty.
fn build_field_list(list: Option<&FieldListContextAll<'_>>) -> Result<Vec<String>, PplError> {
    list.map_or_else(
        || Ok(Vec::new()),
        |list| list.fieldExpression_all().iter().map(|field| name_field(field)).collect(),
    )
}

/// A `dedup` command; `consecutive=true` and a count below 1 are refused.
fn build_dedup(dedup: &DedupCommandContextAll<'_>) -> Result<DedupCommand, PplError> {
    if dedup
        .consecutive
        .as_deref()
        .is_some_and(|consecutive| consecutive.TRUE().is_some())
    {
        return Err(build_unsupported_option_error("dedup", "consecutive=true"));
    }
    let count = dedup
        .number
        .as_deref()
        .map(|number| read_positive_count(number, "dedup"))
        .transpose()?
        .unwrap_or(DEFAULT_DEDUP_COUNT);
    Ok(DedupCommand {
        count,
        fields: build_field_list(dedup.fieldList().as_deref())?,
        keeps_empty: dedup.keepempty.as_deref().is_some_and(|keeps| keeps.TRUE().is_some()),
    })
}

/// A `timechart` command, its parameters applied in the order written over the defaults; a span
/// without a time unit is refused.
fn build_timechart(timechart: &TimechartCommandContextAll<'_>) -> Result<TimechartCommand, PplError> {
    let term = timechart.statsAggTerm().ok_or_else(|| build_missing_part_error("timechart"))?;
    let mut command = TimechartCommand {
        time_field: IMPLICIT_TIME_FIELD.to_string(),
        span_length: DEFAULT_TIMECHART_SPAN_LENGTH,
        span_unit: DEFAULT_TIMECHART_SPAN_UNIT,
        limit: DEFAULT_TIMECHART_LIMIT,
        uses_other: true,
        aggregation: build_aggregation(&term)?,
        by: timechart.fieldExpression().as_deref().map(name_field).transpose()?,
    };
    for parameter in timechart.timechartParameter_all() {
        if parameter.LIMIT().is_some() {
            let limit = parameter
                .integerLiteral()
                .ok_or_else(|| build_missing_part_error("timechart limit"))?;
            command.limit = read_count(&limit, "timechart limit")?;
        } else if parameter.SPAN().is_some() {
            let span = parameter
                .spanLiteral()
                .ok_or_else(|| build_missing_part_error("timechart span"))?;
            let (length, unit) = read_span_literal(&span)?;
            if unit == SpanUnit::Numeric {
                return Err(PplError::Semantic {
                    reason: format!("timechart span [{}] needs a time unit", span.get_text()),
                });
            }
            command.span_length = length;
            command.span_unit = unit;
        } else if parameter.USEOTHER().is_some() {
            command.uses_other = match parameter.booleanLiteral() {
                Some(value) => value.TRUE().is_some(),
                None => read_flag_identifier(&parameter.get_text())?,
            };
        } else if parameter.TIMEFIELD().is_some() {
            let time_field = match (parameter.ident(), parameter.stringLiteral()) {
                (Some(ident), _) => unquote_identifier(&ident),
                (None, Some(text)) => unquote_string(&text),
                (None, None) => return Err(build_missing_part_error("timechart timefield")),
            };
            command.time_field = check_field_name(time_field)?;
        }
    }
    Ok(command)
}

/// `useother=t` / `useother=f`: an identifier value read as a boolean.
fn read_flag_identifier(parameter: &str) -> Result<bool, PplError> {
    let value = parameter.rsplit('=').next().unwrap_or_default().to_ascii_lowercase();
    match value.as_str() {
        "t" | "true" => Ok(true),
        "f" | "false" => Ok(false),
        _ => Err(PplError::Semantic {
            reason: format!("[{parameter}] is not true or false"),
        }),
    }
}

/// A `bin` command; `as`, every option but `span`, and a `bin` without `span` are refused.
fn build_bin(bin: &BinCommandContextAll<'_>) -> Result<BinCommand, PplError> {
    if bin.alias.is_some() {
        return Err(build_unsupported_option_error("bin", "as"));
    }
    let field = bin.fieldExpression().ok_or_else(|| build_missing_part_error("bin"))?;
    let mut span = None;
    for option in bin.binOption_all() {
        let Some(value) = option.span.as_deref() else {
            let name = option.start().get_text().to_ascii_lowercase();
            return Err(build_unsupported_option_error("bin", &name));
        };
        span = Some(read_bin_span(value)?);
    }
    let span = span.ok_or_else(|| PplError::Semantic {
        reason: "bin needs span=<width>".to_string(),
    })?;
    Ok(BinCommand {
        field: name_field(&field)?,
        span,
    })
}

/// A `bin` width: a number, or `<digits><unit>` of [`parse_bin_unit`].
fn read_bin_span(value: &BinSpanValueContextAll<'_>) -> Result<BinSpan, PplError> {
    let BinSpanValueContextAll::NumericSpanValueContext(numeric) = value else {
        return Err(build_unsupported_option_error("bin", "span=log"));
    };
    let literal = numeric.spanLiteral().ok_or_else(|| build_missing_part_error("bin span"))?;
    Ok(match split_span_text(&literal)? {
        SpanText::Numeric(width) => BinSpan::Numeric(width),
        SpanText::Time { length, unit_text } => BinSpan::Time {
            length,
            unit: parse_bin_unit(&unit_text)?,
        },
    })
}

/// A count that must be at least 1 (`dedup N`, `top N`).
fn read_positive_count(literal: &IntegerLiteralContextAll<'_>, command: &str) -> Result<usize, PplError> {
    let count = read_count(literal, command)?;
    if count == 0 {
        return Err(PplError::Semantic {
            reason: format!("{command} takes a count of at least 1"),
        });
    }
    Ok(count)
}

/// The refusal of a span or bin length that is zero, negative or not a finite number.
fn build_invalid_span_error(text: &str) -> PplError {
    PplError::Semantic {
        reason: format!("span length [{text}] must be positive"),
    }
}

/// The refusal of a span or bin length, written as `text`, beyond the range its command holds.
fn build_out_of_range_span_error(text: &str) -> PplError {
    PplError::Semantic {
        reason: format!("span length [{text}] is out of range"),
    }
}

/// The refusal of an option `command` accepts in the grammar but IceGate does not plan.
fn build_unsupported_option_error(command: &str, option: &str) -> PplError {
    PplError::Semantic {
        reason: format!("{command} option [{option}] is not supported"),
    }
}

fn build_assignment(clause: &EvalClauseContextAll<'_>) -> Result<(String, PplExpr), PplError> {
    let field = clause.fieldExpression().ok_or_else(|| build_missing_part_error("eval"))?;
    let value = clause.logicalExpression().ok_or_else(|| build_missing_part_error("eval"))?;
    Ok((name_field(&field)?, build_logical_expression(&value)?))
}

/// One `<field> as <name>` pair. Wildcard renames are refused: a pattern pair would rename an
/// unbounded set of fields.
fn build_rename(clause: &RenameClauseContextAll<'_>) -> Result<(String, String), PplError> {
    let original_field = clause
        .orignalField
        .as_deref()
        .ok_or_else(|| build_missing_part_error("rename"))?;
    let new_field = clause
        .renamedField
        .as_deref()
        .ok_or_else(|| build_missing_part_error("rename"))?;
    Ok((name_rename_field(original_field)?, name_rename_field(new_field)?))
}

/// The field name of one side of a `rename` pair; `*` and a wildcard pattern are refused.
fn name_rename_field(field: &RenameFieldExpressionContextAll<'_>) -> Result<String, PplError> {
    let qualified_name = field.wcQualifiedName().ok_or_else(|| PplError::Semantic {
        reason: "rename takes field names, not *".to_string(),
    })?;
    let name = name_wildcard_path(&qualified_name)?;
    if name.contains('*') {
        return Err(PplError::Semantic {
            reason: format!("rename takes field names, not the pattern [{name}]"),
        });
    }
    Ok(name)
}

fn build_fields(body: &FieldsCommandBodyContextAll<'_>) -> Result<PplCommand, PplError> {
    let list = body.wcFieldList().ok_or_else(|| build_missing_part_error("fields"))?;
    let fields = list
        .selectFieldExpression_all()
        .iter()
        .map(|field| name_select_field(field))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PplCommand::Fields {
        is_exclusion: body.MINUS().is_some(),
        fields,
    })
}

fn name_select_field(field: &SelectFieldExpressionContextAll<'_>) -> Result<String, PplError> {
    field
        .wcQualifiedName()
        .map_or_else(|| Ok("*".to_string()), |name| name_wildcard_path(&name))
}

/// The dotted path of a wildcard-capable field name or pattern. `*` inside an identifier is the
/// wildcard; the grammar's `%` wildcard form is refused, so a pattern means one thing. A reserved
/// name is refused too ([`check_field_name`]).
fn name_wildcard_path(name: &WcQualifiedNameContextAll<'_>) -> Result<String, PplError> {
    let WcQualifiedNameContextAll::IdentsAsWildcardQualifiedNameContext(name) = name else {
        return Err(PplError::Internal {
            reason: "an unlabeled wildcard name".to_string(),
        });
    };
    let parts = name
        .wildcard_all()
        .iter()
        .map(|part| name_wildcard(part))
        .collect::<Result<Vec<_>, _>>()?;
    check_field_name(parts.join("."))
}

fn name_wildcard(wildcard: &WildcardContextAll<'_>) -> Result<String, PplError> {
    if let Some(nested_wildcard) = wildcard.wildcard() {
        return name_wildcard(&nested_wildcard);
    }
    if !wildcard.MODULE_all().is_empty() {
        return Err(PplError::Semantic {
            reason: format!(
                "field pattern [{}] uses % as a wildcard; use * instead",
                wildcard.get_text()
            ),
        });
    }
    let idents = wildcard.ident_all();
    Ok(idents.iter().map(|ident| unquote_identifier(ident)).collect::<String>())
}

fn build_sort_key(field: &SortFieldContextAll<'_>) -> Result<PplSortKey, PplError> {
    let (expression, is_descending) = match field {
        SortFieldContextAll::InvalidMixedSortFieldContext(_) => {
            return Err(PplError::Semantic {
                reason: format!(
                    "sort key [{}] has both a +/- prefix and an asc/desc suffix; use one",
                    field.get_text()
                ),
            });
        }
        SortFieldContextAll::PrefixSortFieldContext(prefix_key) => {
            (prefix_key.sortFieldExpression(), prefix_key.MINUS().is_some())
        }
        SortFieldContextAll::SuffixSortFieldContext(suffix_key) => (
            suffix_key.sortFieldExpression(),
            suffix_key.DESC().is_some() || suffix_key.D().is_some(),
        ),
        SortFieldContextAll::DefaultSortFieldContext(default_key) => (default_key.sortFieldExpression(), false),
        SortFieldContextAll::Error(_) => {
            return Err(PplError::Internal {
                reason: "an unlabeled sort key".to_string(),
            });
        }
    };
    let expression = expression.ok_or_else(|| build_missing_part_error("sort"))?;
    Ok(PplSortKey {
        field: name_sort_field(&expression)?,
        is_descending,
    })
}

fn name_sort_field(expression: &SortFieldExpressionContextAll<'_>) -> Result<String, PplError> {
    let field = expression.fieldExpression().ok_or_else(|| build_missing_part_error("sort"))?;
    if expression.get_child_count() > 1 {
        return Err(refuse_function_call(expression));
    }
    name_field(&field)
}

/// A non-negative count written as an integer literal (`head 5`, `head 5 from 10`).
fn read_count(literal: &IntegerLiteralContextAll<'_>, command: &str) -> Result<usize, PplError> {
    let value = read_integer(literal)?;
    usize::try_from(value).map_err(|_| PplError::Semantic {
        reason: format!("{command} takes a non-negative count, not {value}"),
    })
}

fn read_integer(literal: &IntegerLiteralContextAll<'_>) -> Result<i64, PplError> {
    let text = literal.get_text();
    text.parse::<i64>().map_err(|_| PplError::Semantic {
        reason: format!("integer literal [{text}] is out of range"),
    })
}

/// Build a predicate or value from a `logicalExpression`.
fn build_logical_expression(expression: &LogicalExpressionContextAll<'_>) -> Result<PplExpr, PplError> {
    match expression {
        LogicalExpressionContextAll::LogicalNotContext(not) => {
            let operand = not.logicalExpression().ok_or_else(|| build_missing_part_error("not"))?;
            Ok(PplExpr::Not(Box::new(build_logical_expression(&operand)?)))
        }
        LogicalExpressionContextAll::LogicalAndContext(and) => {
            let (left, right) = build_operands(and.left.as_deref(), and.right.as_deref(), "and")?;
            Ok(PplExpr::And(left, right))
        }
        LogicalExpressionContextAll::LogicalOrContext(or) => {
            let (left, right) = build_operands(or.left.as_deref(), or.right.as_deref(), "or")?;
            Ok(PplExpr::Or(left, right))
        }
        LogicalExpressionContextAll::LogicalXorContext(xor) => {
            let (left, right) = build_operands(xor.left.as_deref(), xor.right.as_deref(), "xor")?;
            Ok(PplExpr::Xor(left, right))
        }
        LogicalExpressionContextAll::LogicalExprContext(expression_context) => {
            let inner_expression = expression_context
                .expression()
                .ok_or_else(|| build_missing_part_error("expression"))?;
            build_expression(&inner_expression)
        }
        LogicalExpressionContextAll::Error(_) => Err(PplError::Internal {
            reason: "an unlabeled logical expression".to_string(),
        }),
    }
}

fn build_operands(
    left: Option<&LogicalExpressionContextAll<'_>>,
    right: Option<&LogicalExpressionContextAll<'_>>,
    operator: &str,
) -> Result<(Box<PplExpr>, Box<PplExpr>), PplError> {
    let left = left.ok_or_else(|| build_missing_part_error(operator))?;
    let right = right.ok_or_else(|| build_missing_part_error(operator))?;
    Ok((
        Box::new(build_logical_expression(left)?),
        Box::new(build_logical_expression(right)?),
    ))
}

fn build_expression(expression: &ExpressionContextAll<'_>) -> Result<PplExpr, PplError> {
    match expression {
        ExpressionContextAll::ValueExprContext(value) => {
            let value_expression = value.valueExpression().ok_or_else(|| build_missing_part_error("value"))?;
            build_value(&value_expression)
        }
        ExpressionContextAll::CompareExprContext(compare) => {
            let operator = compare
                .comparisonOperator()
                .ok_or_else(|| build_missing_part_error("comparison"))?;
            let left = compare.left.as_deref().ok_or_else(|| build_missing_part_error("comparison"))?;
            let right = compare.right.as_deref().ok_or_else(|| build_missing_part_error("comparison"))?;
            let (left, right) = (build_expression(left)?, build_expression(right)?);
            // `a LIKE b` is the two-argument `like(a, b)`, case-insensitive like the function.
            if operator.LIKE().is_some() {
                return Ok(PplExpr::FunctionCall {
                    name: "like".to_string(),
                    arguments: vec![left, right],
                });
            }
            Ok(PplExpr::Compare {
                operator: read_compare_operator(&operator)?,
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        ExpressionContextAll::RelevanceExprContext(relevance) => Err(refuse_function_call(relevance)),
        ExpressionContextAll::InExprContext(membership) => {
            let value = membership.expression().ok_or_else(|| build_missing_part_error("in"))?;
            let value_list = membership.valueList().ok_or_else(|| build_missing_part_error("in"))?;
            let list = value_list
                .literalValue_all()
                .iter()
                .map(|literal| build_literal(literal).map(PplExpr::Literal))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PplExpr::In {
                value: Box::new(build_expression(&value)?),
                list,
                is_negated: membership.NOT().is_some(),
            })
        }
        ExpressionContextAll::BetweenContext(between) => {
            let operands = between.expression_all();
            let [value, low, high] = operands.as_slice() else {
                return Err(build_missing_part_error("between"));
            };
            Ok(PplExpr::Between {
                value: Box::new(build_expression(value)?),
                low: Box::new(build_expression(low)?),
                high: Box::new(build_expression(high)?),
                is_negated: between.NOT().is_some(),
            })
        }
        ExpressionContextAll::IsNullPredicateContext(null_test) => {
            let value = null_test.expression().ok_or_else(|| build_missing_part_error("is null"))?;
            let null_keyword = null_test.nullNotnull().ok_or_else(|| build_missing_part_error("is null"))?;
            Ok(PplExpr::IsNull {
                value: Box::new(build_expression(&value)?),
                is_negated: null_keyword.NOT().is_some(),
            })
        }
        ExpressionContextAll::Error(_) => Err(PplError::Internal {
            reason: "an unlabeled expression".to_string(),
        }),
    }
}

fn read_compare_operator(operator: &ComparisonOperatorContextAll<'_>) -> Result<CompareOperator, PplError> {
    if operator.EQUAL().is_some() || operator.DOUBLE_EQUAL().is_some() {
        return Ok(CompareOperator::Equal);
    }
    if operator.NOT_EQUAL().is_some() {
        return Ok(CompareOperator::NotEqual);
    }
    if operator.LESS().is_some() {
        return Ok(CompareOperator::Less);
    }
    if operator.NOT_GREATER().is_some() {
        return Ok(CompareOperator::LessOrEqual);
    }
    if operator.GREATER().is_some() {
        return Ok(CompareOperator::Greater);
    }
    if operator.NOT_LESS().is_some() {
        return Ok(CompareOperator::GreaterOrEqual);
    }
    Err(build_unsupported_function_error(
        &operator.get_text().to_ascii_lowercase(),
    ))
}

fn build_value(value: &ValueExpressionContextAll<'_>) -> Result<PplExpr, PplError> {
    match value {
        ValueExpressionContextAll::LiteralValueExprContext(literal) => {
            let literal = literal.literalValue().ok_or_else(|| build_missing_part_error("literal"))?;
            Ok(PplExpr::Literal(build_literal(&literal)?))
        }
        ValueExpressionContextAll::FieldExprContext(field) => {
            let field = field.fieldExpression().ok_or_else(|| build_missing_part_error("field"))?;
            Ok(PplExpr::Field(name_field(&field)?))
        }
        ValueExpressionContextAll::NestedValueExprContext(nested) => {
            let nested_expression = nested
                .logicalExpression()
                .ok_or_else(|| build_missing_part_error("parenthesis"))?;
            build_logical_expression(&nested_expression)
        }
        ValueExpressionContextAll::BinaryArithmeticContext(arithmetic) => {
            let operator_token = arithmetic
                .binaryOperator
                .as_ref()
                .ok_or_else(|| build_missing_part_error("arithmetic"))?;
            let operator = match operator_token.get_text() {
                "+" => ArithmeticOperator::Add,
                "-" => ArithmeticOperator::Subtract,
                "*" => ArithmeticOperator::Multiply,
                "/" => ArithmeticOperator::Divide,
                "%" => ArithmeticOperator::Modulo,
                other => return Err(build_unsupported_function_error(other)),
            };
            let left = arithmetic
                .left
                .as_deref()
                .ok_or_else(|| build_missing_part_error("arithmetic"))?;
            let right = arithmetic
                .right
                .as_deref()
                .ok_or_else(|| build_missing_part_error("arithmetic"))?;
            Ok(PplExpr::Arithmetic {
                operator,
                left: Box::new(build_value(left)?),
                right: Box::new(build_value(right)?),
            })
        }
        ValueExpressionContextAll::FunctionCallExprContext(call) => {
            let call = call.functionCall().ok_or_else(|| build_missing_part_error("function call"))?;
            build_function_call(&call)
        }
        ValueExpressionContextAll::LambdaExprContext(lambda) => {
            let lambda = lambda.lambda().ok_or_else(|| build_missing_part_error("lambda"))?;
            build_lambda(&lambda)
        }
        ValueExpressionContextAll::ForeachPlaceholderExprContext(_) => Err(build_unsupported_command_error("foreach")),
        ValueExpressionContextAll::InSubqueryExprContext(in_subsearch) => build_in_subsearch(in_subsearch),
        ValueExpressionContextAll::ScalarSubqueryExprContext(_)
        | ValueExpressionContextAll::ExistsSubqueryExprContext(_) => Err(build_unsupported_command_error("subsearch")),
        ValueExpressionContextAll::Error(_) => Err(PplError::Internal {
            reason: "an unlabeled value expression".to_string(),
        }),
    }
}

/// Builds `<expression> in [<subsearch>]`.
///
/// # Errors
///
/// `Semantic` for `not in [ … ]` and for the tuple form `(a, b) in [ … ]`.
fn build_in_subsearch(in_subsearch: &InSubqueryExprContext<'_>) -> Result<PplExpr, PplError> {
    if in_subsearch.NOT().is_some() {
        return Err(PplError::Semantic {
            reason: "`not in [subsearch]` is not supported; filter inside the subsearch instead".to_string(),
        });
    }
    let values = in_subsearch.valueExpression_all();
    let [value] = values.as_slice() else {
        return Err(PplError::Semantic {
            reason: format!(
                "`(a, b) in [subsearch]` is not supported; compare one expression, not {}",
                values.len()
            ),
        });
    };
    let subsearch = in_subsearch.subSearch().ok_or_else(|| build_missing_part_error("in"))?;
    Ok(PplExpr::InSubsearch {
        expression: Box::new(build_value(value)?),
        query: Box::new(build_subsearch(&subsearch)?),
    })
}

fn build_function_call(call: &FunctionCallContextAll<'_>) -> Result<PplExpr, PplError> {
    if let Some(eval) = call.evalFunctionCall() {
        let name = eval
            .evalFunctionName()
            .ok_or_else(|| build_missing_part_error("function call"))?;
        let arguments = match eval.functionArgs() {
            Some(arguments) => build_arguments(&arguments)?,
            None => Vec::new(),
        };
        return Ok(PplExpr::FunctionCall {
            name: name.get_text().to_ascii_lowercase(),
            arguments,
        });
    }
    if let Some(cast) = call.dataTypeFunctionCall() {
        let value = cast.logicalExpression().ok_or_else(|| build_missing_part_error("cast"))?;
        let target = cast.convertedDataType().ok_or_else(|| build_missing_part_error("cast"))?;
        return Ok(PplExpr::Cast {
            value: Box::new(build_logical_expression(&value)?),
            target: read_cast_target(&target.get_text())?,
        });
    }
    if let Some(case) = call.caseFunctionCall() {
        return build_case(&case);
    }
    if let Some(timestamp) = call.timestampFunctionCall() {
        let name = timestamp
            .timestampFunctionName()
            .ok_or_else(|| build_missing_part_error("timestamp function"))?
            .get_text()
            .to_ascii_lowercase();
        let unit = timestamp
            .simpleDateTimePart()
            .ok_or_else(|| build_missing_part_error("timestamp function"))?;
        let first_argument = timestamp
            .firstArg
            .as_deref()
            .ok_or_else(|| build_missing_part_error("timestamp function"))?;
        let second_argument = timestamp
            .secondArg
            .as_deref()
            .ok_or_else(|| build_missing_part_error("timestamp function"))?;
        // The grammar spells the unit as a keyword; the library reads it as a string literal.
        return Ok(PplExpr::FunctionCall {
            name,
            arguments: vec![
                PplExpr::Literal(PplLiteral::String(unit.get_text().to_ascii_uppercase())),
                build_argument(first_argument)?,
                build_argument(second_argument)?,
            ],
        });
    }
    Err(refuse_function_call(call))
}

fn build_arguments(arguments: &FunctionArgsContextAll<'_>) -> Result<Vec<PplExpr>, PplError> {
    arguments
        .functionArg_all()
        .iter()
        .map(|argument| build_argument(argument))
        .collect()
}

fn build_argument(argument: &FunctionArgContextAll<'_>) -> Result<PplExpr, PplError> {
    let expression = argument
        .functionArgExpression()
        .ok_or_else(|| build_missing_part_error("argument"))?;
    if let Some(lambda) = expression.lambda() {
        return build_lambda(&lambda);
    }
    let value = expression
        .logicalExpression()
        .ok_or_else(|| build_missing_part_error("argument"))?;
    build_logical_expression(&value)
}

fn build_lambda(lambda: &LambdaContextAll<'_>) -> Result<PplExpr, PplError> {
    let body = lambda.logicalExpression().ok_or_else(|| build_missing_part_error("lambda"))?;
    Ok(PplExpr::Lambda {
        parameters: lambda.ident_all().iter().map(|ident| unquote_identifier(ident)).collect(),
        body: Box::new(build_logical_expression(&body)?),
    })
}

/// `case(c1, v1, c2, v2, … [else v])`: the grammar lists the conditions as logical
/// expressions and the values (the `else` value last) as value expressions.
fn build_case(case: &CaseFunctionCallContextAll<'_>) -> Result<PplExpr, PplError> {
    let conditions = case.logicalExpression_all();
    let values = case.valueExpression_all();
    let has_else = case.ELSE().is_some();
    if values.len() != conditions.len() + usize::from(has_else) {
        return Err(build_missing_part_error("case"));
    }
    let branches = conditions
        .iter()
        .zip(&values)
        .map(|(condition, value)| Ok((build_logical_expression(condition)?, build_value(value)?)))
        .collect::<Result<Vec<_>, PplError>>()?;
    let otherwise = if has_else {
        let else_value = values.last().ok_or_else(|| build_missing_part_error("case"))?;
        Some(Box::new(build_value(else_value)?))
    } else {
        None
    };
    Ok(PplExpr::Case { branches, otherwise })
}

/// The `cast` target named `type_name`; a type IceGate does not cast to is refused as the
/// function `cast as <type>`.
fn read_cast_target(type_name: &str) -> Result<CastTarget, PplError> {
    match type_name.to_ascii_uppercase().as_str() {
        "DOUBLE" | "FLOAT" => Ok(CastTarget::Double),
        "LONG" => Ok(CastTarget::Long),
        "INT" | "INTEGER" => Ok(CastTarget::Int),
        "STRING" => Ok(CastTarget::String),
        "BOOLEAN" => Ok(CastTarget::Boolean),
        "TIMESTAMP" => Ok(CastTarget::Timestamp),
        other => Err(build_unsupported_function_error(&format!(
            "cast as {}",
            other.to_ascii_lowercase()
        ))),
    }
}

fn build_literal(literal: &LiteralValueContextAll<'_>) -> Result<PplLiteral, PplError> {
    if let Some(string) = literal.stringLiteral() {
        return Ok(PplLiteral::String(unquote_string(&string)));
    }
    if let Some(integer) = literal.integerLiteral() {
        return Ok(PplLiteral::Integer(read_integer(&integer)?));
    }
    if let Some(boolean) = literal.booleanLiteral() {
        return Ok(PplLiteral::Boolean(boolean.TRUE().is_some()));
    }
    let decimal = literal
        .decimalLiteral()
        .map(|decimal| decimal.get_text())
        .or_else(|| literal.doubleLiteral().map(|double| double.get_text()))
        .or_else(|| literal.floatLiteral().map(|float| float.get_text()));
    if let Some(text) = decimal {
        let digits = text.trim_end_matches(['d', 'D', 'f', 'F']);
        return digits.parse::<f64>().map(PplLiteral::Double).map_err(|_| PplError::Semantic {
            reason: format!("numeric literal [{text}] is not a number"),
        });
    }
    if literal.intervalLiteral().is_some() {
        return Err(build_unsupported_function_error("interval"));
    }
    Err(refuse_function_call(literal))
}

/// The dotted path of a field reference, backticks removed; a reserved name is refused
/// ([`check_field_name`]).
fn name_field(field: &FieldExpressionContextAll<'_>) -> Result<String, PplError> {
    let name = field.qualifiedName().ok_or_else(|| build_missing_part_error("field"))?;
    check_field_name(join_qualified_name(&name)?)
}

/// `name`, a field name a query writes (a reference, a pattern, an `eval` or `rename` target, an
/// alias, a field parameter), when it is not reserved.
///
/// # Errors
///
/// Returns [`PplError::Semantic`], naming it, for a name that starts with
/// [`RESERVED_NAME_PREFIX`]: the planner names its own columns with that prefix, so such a field
/// would collide with one of them or vanish with the hidden ones.
fn check_field_name(name: String) -> Result<String, PplError> {
    if name.starts_with(RESERVED_NAME_PREFIX) {
        return Err(PplError::Semantic {
            reason: format!("field name [{name}] starts with the reserved prefix [{RESERVED_NAME_PREFIX}]"),
        });
    }
    Ok(name)
}

fn join_qualified_name(name: &QualifiedNameContextAll<'_>) -> Result<String, PplError> {
    let QualifiedNameContextAll::IdentsAsQualifiedNameContext(name) = name else {
        return Err(PplError::Internal {
            reason: "an unlabeled qualified name".to_string(),
        });
    };
    Ok(name
        .ident_all()
        .iter()
        .map(|ident| unquote_identifier(ident))
        .collect::<Vec<_>>()
        .join("."))
}

/// The name an identifier stands for. Inside backticks, a doubled backtick stands for one
/// backtick, so a MAP key containing a backtick is written ``` `a``b` ```; every other character,
/// a backslash included, stands for itself. A leading dot (`.kibana`) is part of the name.
fn unquote_identifier(ident: &IdentContextAll<'_>) -> String {
    if let Some(nested_ident) = ident.ident() {
        return unquote_identifier(&nested_ident);
    }
    let text = ident.get_text();
    match ident.BQUOTA_STRING() {
        Some(_) => text
            .strip_prefix('`')
            .and_then(|quoted| quoted.strip_suffix('`'))
            .unwrap_or(&text)
            .replace("``", "`"),
        None => text,
    }
}

/// The value a quoted string literal stands for. Inside the quotes, a doubled quote of the
/// enclosing kind stands for one quote, and a backslash before `'`, `"` or another backslash
/// stands for that character; every other backslash stays.
fn unquote_string(literal: &StringLiteralContextAll<'_>) -> String {
    let text = literal.get_text();
    let mut characters = text.chars();
    let Some(quote) = characters.next() else {
        return text;
    };
    let quoted_characters: Vec<char> = characters.collect();
    let content = quoted_characters.strip_suffix(&[quote]).unwrap_or(&quoted_characters);
    let mut value = String::with_capacity(content.len());
    let mut index = 0;
    while index < content.len() {
        let character = content[index];
        let next = content.get(index + 1).copied();
        let is_escape = character == '\\' && matches!(next, Some('\'' | '"' | '\\'));
        let is_doubled_quote = character == quote && next == Some(quote);
        if is_escape || is_doubled_quote {
            index += 1;
            value.push(content[index]);
        } else {
            value.push(character);
        }
        index += 1;
    }
    value
}

/// The command name of the first child rule of `context`: its rule name without the `Command`
/// suffix, lower-cased (`joinCommand` is `join`, `appendPipeCommand` is `appendpipe`).
fn name_first_child_rule<'input, T>(context: &T) -> String
where
    T: ParserRuleContext<'input>,
{
    context
        .get_child(0)
        .and_then(|child| ruleNames.get(child.get_rule_index()).copied())
        .map_or_else(
            || context.get_text().to_ascii_lowercase(),
            |rule| rule.trim_end_matches("Command").to_ascii_lowercase(),
        )
}

/// The unsupported-function error naming a call by the text before its parenthesis (`match`,
/// `simple_query_string`).
fn refuse_function_call<'input, T>(context: &T) -> PplError
where
    T: ParseTree<'input>,
{
    let text = context.get_text();
    let name = text.split('(').next().unwrap_or(&text);
    build_unsupported_function_error(&name.to_ascii_lowercase())
}

fn build_unsupported_command_error(command: &str) -> PplError {
    PplError::UnsupportedCommand {
        command: command.to_string(),
    }
}

fn build_unsupported_function_error(function: &str) -> PplError {
    PplError::UnsupportedFunction {
        function: function.to_string(),
    }
}

/// A `join` option written twice: refused, because neither value can be preferred over the other.
fn build_repeated_join_option_error(option: &str) -> PplError {
    PplError::Semantic {
        reason: format!("join option {option}= is written more than once; write it once"),
    }
}

/// A parse tree missing a part its rule requires: the grammar guarantees it, so this is a defect.
fn build_missing_part_error(part: &str) -> PplError {
    PplError::Internal {
        reason: format!("the parse tree of a PPL {part} is missing a required part"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unplanned_command_is_a_400_naming_it() {
        let cases = [
            ("source=logs | patterns body", "patterns"),
            ("source=logs | lookup prices model", "lookup"),
            ("source=logs | grok body 'client=.*'", "grok"),
            ("source=logs | trendline sma(2, severity_number)", "trendline"),
            ("source=logs | flatten resource_attributes", "flatten"),
        ];
        for (query, command) in cases {
            assert_eq!(
                parse_ppl(query),
                Err(PplError::UnsupportedCommand {
                    command: command.to_string()
                }),
                "query {query:?}"
            );
        }
        // `take` is an aggregation of `stats` in the grammar; its refusal names it.
        assert_eq!(
            parse_ppl("source=logs | stats take(body, 5)"),
            Err(PplError::UnsupportedFunction {
                function: "take".to_string()
            })
        );
    }

    #[test]
    fn describe_parse_and_fillnull_build_their_ast_nodes() {
        let describe_query = parse_ppl("describe logs").expect("describe parses");
        assert_eq!(
            describe_query,
            PplQuery {
                source: PplSource::Describe {
                    index: "logs".to_string()
                },
                commands: vec![],
            }
        );

        let parse_query = parse_ppl("source=logs | parse body '(?<host>[a-z]+) .*'").expect("parse parses");
        assert_eq!(
            parse_query.commands,
            vec![PplCommand::Parse(ParseCommand {
                source: PplExpr::Field("body".to_string()),
                pattern: "(?<host>[a-z]+) .*".to_string(),
            })]
        );

        let text = |value: &str| PplExpr::Literal(PplLiteral::String(value.to_string()));
        let fill = |replacements: FillnullReplacements| vec![PplCommand::Fillnull(FillnullCommand { replacements })];
        for (query, expected_commands) in [
            (
                "source=logs | fillnull with 'x' in service_name, body",
                fill(FillnullReplacements::Fields(vec![
                    ("service_name".to_string(), text("x")),
                    ("body".to_string(), text("x")),
                ])),
            ),
            (
                "source=logs | fillnull using service_name = 'a', body = 'b'",
                fill(FillnullReplacements::Fields(vec![
                    ("service_name".to_string(), text("a")),
                    ("body".to_string(), text("b")),
                ])),
            ),
            (
                "source=logs | fillnull value='x' service_name body",
                fill(FillnullReplacements::Fields(vec![
                    ("service_name".to_string(), text("x")),
                    ("body".to_string(), text("x")),
                ])),
            ),
            (
                "source=logs | fillnull with 'x'",
                fill(FillnullReplacements::AllFields(text("x"))),
            ),
            (
                "source=logs | fillnull value=0",
                fill(FillnullReplacements::AllFields(PplExpr::Literal(PplLiteral::Integer(
                    0,
                )))),
            ),
        ] {
            let query_ast = parse_ppl(query).expect("fillnull parses");
            assert_eq!(query_ast.commands, expected_commands, "{query}");
        }

        // A field name the query writes or reads keeps the reserved-prefix refusal in both commands.
        for query in [
            "source=logs | parse __ppl_x '(?<a>.*)'",
            "source=logs | fillnull using __ppl_x = 'a'",
            "source=logs | fillnull with 'a' in body, __ppl_x",
        ] {
            assert_eq!(
                parse_ppl(query),
                Err(PplError::Semantic {
                    reason: format!("field name [__ppl_x] starts with the reserved prefix [{RESERVED_NAME_PREFIX}]"),
                }),
                "{query}"
            );
        }
    }

    #[test]
    fn search_source_and_index_forms_build_the_same_query() {
        let expected = PplQuery {
            source: PplSource::Index {
                expression: "logs,spans".to_string(),
            },
            commands: vec![PplCommand::Head { count: 10, offset: 0 }],
        };
        for query in [
            "source=logs,spans | head",
            "search source=logs,spans | head",
            "index = logs, spans | HEAD",
        ] {
            assert_eq!(parse_ppl(query), Ok(expected.clone()), "query {query:?}");
        }
    }

    #[test]
    fn a_query_without_a_source_is_a_syntax_error() {
        for query in ["", "   ", "where severity_text = 'ERROR'"] {
            assert!(
                matches!(parse_ppl(query), Err(PplError::Syntax { .. })),
                "query {query:?}"
            );
        }
    }

    #[test]
    fn core_commands_build_their_ast() {
        let query = parse_ppl(
            "source=logs | where severity_text = 'ERROR' and not service_name != \"api\" \
             | fields - body, log_attributes.`http.method` | sort - timestamp, service_name asc | head 5 from 2",
        );
        let field = |name: &str| Box::new(PplExpr::Field(name.to_string()));
        let text = |value: &str| Box::new(PplExpr::Literal(PplLiteral::String(value.to_string())));
        assert_eq!(
            query,
            Ok(PplQuery {
                source: PplSource::Index {
                    expression: "logs".to_string()
                },
                commands: vec![
                    PplCommand::Where(PplExpr::And(
                        Box::new(PplExpr::Compare {
                            operator: CompareOperator::Equal,
                            left: field("severity_text"),
                            right: text("ERROR"),
                        }),
                        Box::new(PplExpr::Not(Box::new(PplExpr::Compare {
                            operator: CompareOperator::NotEqual,
                            left: field("service_name"),
                            right: text("api"),
                        }))),
                    )),
                    PplCommand::Fields {
                        is_exclusion: true,
                        fields: vec!["body".to_string(), "log_attributes.http.method".to_string()],
                    },
                    PplCommand::Sort(vec![
                        PplSortKey {
                            field: "timestamp".to_string(),
                            is_descending: true
                        },
                        PplSortKey {
                            field: "service_name".to_string(),
                            is_descending: false
                        },
                    ]),
                    PplCommand::Head { count: 5, offset: 2 },
                ],
            })
        );
    }

    #[test]
    fn quoted_strings_and_backtick_identifiers_unquote_as_stated() {
        let query = parse_ppl(r"source=logs | where log_attributes.`a``b` = 'it''s \'x\' \d'");
        let Ok(PplQuery { commands, .. }) = query else {
            panic!("query parses: {query:?}");
        };
        assert_eq!(
            commands,
            vec![PplCommand::Where(PplExpr::Compare {
                operator: CompareOperator::Equal,
                left: Box::new(PplExpr::Field("log_attributes.a`b".to_string())),
                right: Box::new(PplExpr::Literal(PplLiteral::String(r"it's 'x' \d".to_string()))),
            })]
        );
    }

    #[test]
    fn a_mixed_sort_key_and_a_negative_head_are_semantic_errors() {
        for query in [
            "source=logs | sort - service_name desc",
            "source=logs | sort + service_name asc",
            "source=logs | head -1",
            "source=logs | head 5 from -2",
        ] {
            assert!(
                matches!(parse_ppl(query), Err(PplError::Semantic { .. })),
                "query {query:?} is a semantic error"
            );
        }
    }

    #[test]
    fn span_unit_spellings_map_to_their_units() {
        let cases = [
            ("ms", SpanUnit::Millisecond),
            ("MS", SpanUnit::Millisecond),
            ("millisecond", SpanUnit::Millisecond),
            ("s", SpanUnit::Second),
            ("S", SpanUnit::Second),
            ("sec", SpanUnit::Second),
            ("secs", SpanUnit::Second),
            ("second", SpanUnit::Second),
            ("seconds", SpanUnit::Second),
            ("m", SpanUnit::Minute),
            ("min", SpanUnit::Minute),
            ("MINS", SpanUnit::Minute),
            ("minute", SpanUnit::Minute),
            ("minutes", SpanUnit::Minute),
            ("h", SpanUnit::Hour),
            ("hr", SpanUnit::Hour),
            ("hrs", SpanUnit::Hour),
            ("hour", SpanUnit::Hour),
            ("Hours", SpanUnit::Hour),
            ("d", SpanUnit::Day),
            ("D", SpanUnit::Day),
            ("day", SpanUnit::Day),
            ("days", SpanUnit::Day),
            ("w", SpanUnit::Week),
            ("week", SpanUnit::Week),
            ("weeks", SpanUnit::Week),
            ("M", SpanUnit::Month),
            ("mon", SpanUnit::Month),
            ("month", SpanUnit::Month),
            ("months", SpanUnit::Month),
            ("q", SpanUnit::Quarter),
            ("qtr", SpanUnit::Quarter),
            ("qtrs", SpanUnit::Quarter),
            ("quarter", SpanUnit::Quarter),
            ("quarters", SpanUnit::Quarter),
            ("y", SpanUnit::Year),
            ("Y", SpanUnit::Year),
            ("year", SpanUnit::Year),
            ("years", SpanUnit::Year),
        ];
        for (spelling, unit) in cases {
            assert_eq!(parse_span_unit(spelling), Ok(unit), "spelling {spelling:?}");
        }
        for spelling in ["us", "cs", "ds", "milliseconds", "yr", "yrs", "fortnight"] {
            assert!(
                matches!(parse_span_unit(spelling), Err(PplError::Semantic { .. })),
                "spelling {spelling:?}"
            );
        }
    }

    #[test]
    fn bin_unit_spellings_map_to_their_units() {
        let cases = [
            ("us", SpanUnit::Microsecond),
            ("ms", SpanUnit::Millisecond),
            ("MS", SpanUnit::Millisecond),
            ("cs", SpanUnit::Centisecond),
            ("ds", SpanUnit::Decisecond),
            ("s", SpanUnit::Second),
            ("secs", SpanUnit::Second),
            ("m", SpanUnit::Minute),
            ("mins", SpanUnit::Minute),
            ("h", SpanUnit::Hour),
            ("hrs", SpanUnit::Hour),
            ("d", SpanUnit::Day),
            ("days", SpanUnit::Day),
            ("M", SpanUnit::Month),
            ("mon", SpanUnit::Month),
            ("months", SpanUnit::Month),
        ];
        for (spelling, unit) in cases {
            assert_eq!(parse_bin_unit(spelling), Ok(unit), "spelling {spelling:?}");
        }
        for spelling in ["US", "CS", "w", "q", "y", "yr"] {
            assert!(
                matches!(parse_bin_unit(spelling), Err(PplError::Semantic { .. })),
                "spelling {spelling:?}"
            );
        }
    }

    #[test]
    fn refused_aggregating_command_options_are_semantic_errors_with_their_stated_reasons() {
        // One case per refusal the builder states; the reasons are IceGate's wording.
        let cases = [
            (
                "source=logs | dedup 2 service_name consecutive=true",
                "dedup option [consecutive=true] is not supported",
            ),
            (
                "source=logs | dedup 0 service_name",
                "dedup takes a count of at least 1",
            ),
            ("source=logs | top 0 service_name", "top takes a count of at least 1"),
            ("source=logs | rare 0 service_name", "rare takes a count of at least 1"),
            (
                "source=logs | bin severity_number span=10 as b",
                "bin option [as] is not supported",
            ),
            ("source=logs | bin severity_number", "bin needs span=<width>"),
            (
                "source=logs | bin timestamp span=1.5h",
                "span length [1.5h] must be a whole number of units",
            ),
            (
                "source=logs | bin timestamp span=1.5d",
                "span length [1.5d] must be a whole number of units",
            ),
            (
                "source=logs | stats count() by span(timestamp, 1.5h)",
                "span length [1.5h] must be a whole number of units",
            ),
            (
                "source=logs | bin timestamp span=10000000000000000000h",
                "span length [10000000000000000000h] is out of range",
            ),
            // 2^63 days, one more than the greatest 64-bit length.
            (
                "source=logs | bin timestamp span=9223372036854775808d",
                "span length [9223372036854775808d] is out of range",
            ),
            (
                "source=logs | stats count() by span(timestamp, 10000000000000000000M)",
                "span length [10000000000000000000M] is out of range",
            ),
            // One more than 2^53 months: the span length would not be the one written.
            (
                "source=logs | stats count() by span(timestamp, 9007199254740993M)",
                "span length [9007199254740993M] is out of range",
            ),
            (
                "source=logs | stats partitions=2 count()",
                "stats option [partitions] is not supported",
            ),
            (
                "source=logs | stats allnum=true count()",
                "stats option [allnum] is not supported",
            ),
            (
                "source=logs | stats delim=',' count()",
                "stats option [delim] is not supported",
            ),
            (
                "source=logs | stats count() dedup_splitvalues=true",
                "stats option [dedup_splitvalues] is not supported",
            ),
            (
                "source=logs | top showperc=true service_name",
                "top option [showperc] is not supported",
            ),
            (
                "source=logs | top percentfield='p' service_name",
                "top option [percentfield] is not supported",
            ),
            (
                "source=logs | rare percentfield='p' service_name",
                "rare option [percentfield] is not supported",
            ),
        ];
        for (query, reason) in cases {
            assert_eq!(
                parse_ppl(query),
                Err(PplError::Semantic {
                    reason: reason.to_string()
                }),
                "query {query:?}"
            );
        }
        // A bare integer too long for f64 reads as infinity, which is not a width.
        let overflowing_digits = "9".repeat(400);
        for query in [
            format!("source=logs | bin severity_number span={overflowing_digits}"),
            format!("source=logs | stats count() by span(severity_number, {overflowing_digits})"),
        ] {
            assert_eq!(
                parse_ppl(&query),
                Err(PplError::Semantic {
                    reason: format!("span length [{overflowing_digits}] must be positive")
                }),
                "query {query:?}"
            );
        }
    }

    #[test]
    fn a_field_name_starting_with_the_reserved_prefix_is_refused_wherever_a_query_writes_one() {
        // One case per place a query writes a field name: a reference, a pattern, a target, an
        // alias, a parameter value.
        let cases = [
            ("source=logs | where __ppl_x = 'a'", "__ppl_x"),
            ("source=logs | eval y = lower(__ppl_x)", "__ppl_x"),
            ("source=logs | eval __ppl_x = 1", "__ppl_x"),
            ("source=logs | eval `__ppl_x` = 1", "__ppl_x"),
            ("source=logs | rename body as __ppl_x", "__ppl_x"),
            ("source=logs | rename __ppl_x as body", "__ppl_x"),
            ("source=logs | fields body, __ppl_x", "__ppl_x"),
            ("source=logs | fields - __ppl_*", "__ppl_*"),
            ("source=logs | sort - __ppl_x", "__ppl_x"),
            ("source=logs | stats count() as __ppl_x", "__ppl_x"),
            ("source=logs | stats count() by __ppl_x", "__ppl_x"),
            ("source=logs | stats count() by span(__ppl_x, 1h)", "__ppl_x"),
            (
                "source=logs | stats count() by span(timestamp, 1h) as __ppl_x",
                "__ppl_x",
            ),
            ("source=logs | top __ppl_x", "__ppl_x"),
            ("source=logs | rare body by __ppl_x", "__ppl_x"),
            ("source=logs | top countfield='__ppl_x' body", "__ppl_x"),
            ("source=logs | dedup __ppl_x", "__ppl_x"),
            ("source=logs | timechart timefield=__ppl_x count()", "__ppl_x"),
            ("source=logs | timechart timefield='__ppl_x' count()", "__ppl_x"),
            (
                "source=logs | timechart timefield=timestamp count() by __ppl_x",
                "__ppl_x",
            ),
            ("source=logs | bin __ppl_x span=1", "__ppl_x"),
            ("source=logs | join left=__ppl_x on x = y spans", "__ppl_x"),
            ("source=logs | join right=__ppl_x on x = y spans", "__ppl_x"),
            ("source=logs | join on x = y spans as __ppl_x", "__ppl_x"),
        ];
        for (query, name) in cases {
            assert_eq!(
                parse_ppl(query),
                Err(PplError::Semantic {
                    reason: format!("field name [{name}] starts with the reserved prefix [__ppl_]")
                }),
                "query {query:?}"
            );
        }
        // Only a name that starts with the prefix is reserved; a MAP key under a column may carry it.
        assert_eq!(
            parse_ppl("source=logs | eval my__ppl_x = 1 | where log_attributes.__ppl_x = 'a'"),
            Ok(PplQuery {
                source: PplSource::Index {
                    expression: "logs".to_string()
                },
                commands: vec![
                    PplCommand::Eval(vec![(
                        "my__ppl_x".to_string(),
                        PplExpr::Literal(PplLiteral::Integer(1))
                    )]),
                    PplCommand::Where(PplExpr::Compare {
                        operator: CompareOperator::Equal,
                        left: Box::new(PplExpr::Field("log_attributes.__ppl_x".to_string())),
                        right: Box::new(PplExpr::Literal(PplLiteral::String("a".to_string()))),
                    }),
                ],
            })
        );
    }

    #[test]
    fn join_commands_build_their_ast_nodes() {
        let query = parse_ppl(
            "source=logs | left join max=0 left=l right=s on l.service_name = s.service_name \
             [ source=spans | fields name ]",
        )
        .expect("join parses");
        let [PplCommand::Join(join)] = query.commands.as_slice() else {
            panic!("not one join: {:?}", query.commands);
        };
        assert_eq!(join.kind, JoinKind::Left);
        assert_eq!(join.left_alias.as_deref(), Some("l"));
        assert_eq!(join.right_alias.as_deref(), Some("s"));
        assert_eq!(join.max_matches, 0);
        assert_eq!(
            join.criteria,
            PplExpr::Compare {
                operator: CompareOperator::Equal,
                left: Box::new(PplExpr::Field("l.service_name".to_string())),
                right: Box::new(PplExpr::Field("s.service_name".to_string())),
            }
        );
        assert_eq!(
            *join.right,
            PplQuery {
                source: PplSource::Index {
                    expression: "spans".to_string(),
                },
                commands: vec![PplCommand::Fields {
                    is_exclusion: false,
                    fields: vec!["name".to_string()],
                }],
            }
        );
        // Every right side below reads `spans` with no commands, whether written as an index or a subsearch.
        let bare_spans = PplQuery {
            source: PplSource::Index {
                expression: "spans".to_string(),
            },
            commands: Vec::new(),
        };

        for (text, expected_kind, expected_left_alias, expected_right_alias) in [
            (
                "source=logs | join type=left on l.x = r.x spans as r",
                JoinKind::Left,
                None,
                Some("r"),
            ),
            (
                "source=logs | inner join right=r on x = r.x [ source=spans ]",
                JoinKind::Inner,
                None,
                Some("r"),
            ),
            (
                "source=logs | join on x = y [ source=spans ] as r",
                JoinKind::Inner,
                None,
                Some("r"),
            ),
            (
                "source=logs | left outer join left=l on l.x = y spans",
                JoinKind::Left,
                Some("l"),
                None,
            ),
            ("source=logs | outer join where x = y spans", JoinKind::Left, None, None),
            (
                "source=logs | inner join type=inner on x = y spans",
                JoinKind::Inner,
                None,
                None,
            ),
            // `overwrite=` is accepted and changes nothing: the join stays inner and uncapped.
            (
                "source=logs | join overwrite=false on x = y spans",
                JoinKind::Inner,
                None,
                None,
            ),
        ] {
            let parsed_query = parse_ppl(text).expect(text);
            let [PplCommand::Join(join)] = parsed_query.commands.as_slice() else {
                panic!("{text}: not one join: {:?}", parsed_query.commands);
            };
            assert_eq!(join.kind, expected_kind, "{text}");
            assert_eq!(join.left_alias.as_deref(), expected_left_alias, "{text}");
            assert_eq!(join.right_alias.as_deref(), expected_right_alias, "{text}");
            assert_eq!(join.max_matches, 0, "{text}");
            assert_eq!(*join.right, bare_spans, "{text}");
        }
    }

    #[test]
    fn unsupported_join_forms_are_400s_naming_them() {
        for (text, expected_name) in [
            ("source=logs | right join on x = y spans", "right"),
            ("source=logs | full join on x = y spans", "full"),
            ("source=logs | cross join on x = y spans", "cross"),
            ("source=logs | left semi join on x = y spans", "left semi"),
            (
                "source=logs | left outer join type=inner on x = y spans",
                "type=inner contradicts `left outer` written before `join`",
            ),
            ("source=logs | anti join on x = y spans", "anti"),
            (
                "source=logs | left join type=inner on x = y spans",
                "type=inner contradicts `left` written before `join`",
            ),
            (
                "source=logs | join max=-1 on x = y spans",
                "max=-1: max must be a non-negative integer",
            ),
            (
                "source=logs | join max=99999999999999999999999 on x = y spans",
                "max=99999999999999999999999 is out of range",
            ),
            (
                "source=logs | join max=1 max=0 on x = y spans",
                "max= is written more than once",
            ),
            (
                "source=logs | join max=0 max=0 on x = y spans",
                "max= is written more than once",
            ),
            (
                "source=logs | join type=inner type=left on x = y spans",
                "type= is written more than once",
            ),
            (
                "source=logs | join type=left type=left on x = y spans",
                "type= is written more than once",
            ),
            ("source=logs | join hint.left.x=y on a = b spans", "hints"),
            ("source=logs | join [ source=spans ]", "on <"),
        ] {
            let result = parse_ppl(text);
            assert!(
                matches!(&result, Err(PplError::Semantic { reason }) if reason.contains(expected_name)),
                "{text}: {result:?}"
            );
        }
    }

    #[test]
    fn in_subsearches_and_subsearch_sources_build_their_ast_nodes() {
        let filtered_query =
            parse_ppl("source=logs | where trace_id in [ source=spans | where name = 'x' | fields trace_id ]")
                .expect("in subsearch parses");
        let [PplCommand::Where(PplExpr::InSubsearch { expression, query })] = filtered_query.commands.as_slice() else {
            panic!("not one where with a subsearch: {:?}", filtered_query.commands);
        };
        assert!(
            matches!(expression.as_ref(), PplExpr::Field(field) if field == "trace_id"),
            "{expression:?}"
        );
        assert!(
            matches!(&query.source, PplSource::Index { expression } if expression == "spans"),
            "{:?}",
            query.source
        );
        assert_eq!(query.commands.len(), 2, "{:?}", query.commands);

        let aliased_query =
            parse_ppl("source=[ source=logs | where body = 'x' ] as t | fields body").expect("source parses");
        let PplSource::Subsearch {
            query: inner_query,
            alias,
        } = &aliased_query.source
        else {
            panic!("not a subsearch source: {:?}", aliased_query.source);
        };
        assert_eq!(alias.as_deref(), Some("t"));
        assert!(
            matches!(&inner_query.source, PplSource::Index { expression } if expression == "logs"),
            "{:?}",
            inner_query.source
        );
        assert_eq!(inner_query.commands.len(), 1);
        assert_eq!(aliased_query.commands.len(), 1);

        let unaliased_query = parse_ppl("search source=[ source=logs ]").expect("source parses");
        assert!(
            matches!(&unaliased_query.source, PplSource::Subsearch { alias: None, .. }),
            "{:?}",
            unaliased_query.source
        );

        let plain_source = parse_ppl("source=logs | fields body").expect("source parses");
        assert!(
            matches!(&plain_source.source, PplSource::Index { expression } if expression == "logs"),
            "{:?}",
            plain_source.source
        );
    }

    #[test]
    fn an_alias_on_an_index_source_is_a_400() {
        let result = parse_ppl("source=logs as l | fields body");
        assert!(
            matches!(&result, Err(PplError::Semantic { reason })
                if reason.contains("source=logs as l") && reason.contains("left=<alias>")),
            "{result:?}"
        );
    }

    #[test]
    fn not_in_and_tuple_subsearches_are_400s() {
        for (text, expected_name) in [
            (
                "source=logs | where trace_id not in [ source=spans | fields trace_id ]",
                "not in",
            ),
            (
                "source=logs | where (trace_id, span_id) in [ source=spans | fields trace_id, span_id ]",
                "(a, b)",
            ),
        ] {
            let result = parse_ppl(text);
            assert!(
                matches!(&result, Err(PplError::Semantic { reason }) if reason.contains(expected_name)),
                "{text}: {result:?}"
            );
        }
    }
}
