//! The PPL AST builder: walks the parse tree of the vendored grammar into a [`PplQuery`].
//!
//! The grammar accepts every 3.x query. The builder turns the commands and expressions IceGate
//! plans into the AST and refuses every other one by name, so an unplanned construct is a 400
//! naming it, never a silently different query.

use antlr4rust::parser_rule_context::ParserRuleContext;
use antlr4rust::rule_context::CustomRuleContext;
use antlr4rust::token::Token;
use antlr4rust::tree::{ParseTree, Tree};
use icegate_query_grammar::ppl::{
    BetweenContextAttrs, BooleanLiteralContextAttrs, CaseFunctionCallContextAll, CaseFunctionCallContextAttrs,
    CommandsContextAll, CommandsContextAttrs, CompareExprContextAttrs, ComparisonOperatorContextAll,
    ComparisonOperatorContextAttrs, DataTypeFunctionCallContextAttrs, DefaultSortFieldContextAttrs,
    EvalClauseContextAll, EvalClauseContextAttrs, EvalCommandContextAttrs, EvalFunctionCallContextAttrs,
    ExpressionContextAll, FieldExprContextAttrs, FieldExpressionContextAll, FieldExpressionContextAttrs,
    FieldsCommandBodyContextAll, FieldsCommandBodyContextAttrs, FieldsCommandContextAttrs, FromClauseContextAll,
    FromClauseContextAttrs, FunctionArgContextAll, FunctionArgContextAttrs, FunctionArgExpressionContextAttrs,
    FunctionArgsContextAll, FunctionArgsContextAttrs, FunctionCallContextAll, FunctionCallContextAttrs,
    FunctionCallExprContextAttrs, IdentContextAll, IdentContextAttrs, IdentsAsQualifiedNameContextAttrs,
    IdentsAsTableQualifiedNameContextAttrs, IdentsAsWildcardQualifiedNameContextAttrs, InExprContextAttrs,
    IntegerLiteralContextAll, IsNullPredicateContextAttrs, LambdaContextAll, LambdaContextAttrs,
    LambdaExprContextAttrs, LiteralValueContextAll, LiteralValueContextAttrs, LiteralValueExprContextAttrs,
    LogicalExprContextAttrs, LogicalExpressionContextAll, LogicalNotContextAttrs, NestedValueExprContextAttrs,
    NullNotnullContextAttrs, PplCommandsContextAll, PplCommandsContextAttrs, PplStatementContextAttrs,
    PrefixSortFieldContextAttrs, QualifiedNameContextAll, QueryStatementContextAll, QueryStatementContextAttrs,
    RenameClauseContextAll, RenameCommandContextAttrs, RenameFieldExpressionContextAll,
    RenameFieldExpressionContextAttrs, RootContextAttrs, SearchCommandContextAll, SearchFromContextAttrs,
    SelectFieldExpressionContextAll, SelectFieldExpressionContextAttrs, SortCommandContextAttrs, SortFieldContextAll,
    SortFieldExpressionContextAll, SortFieldExpressionContextAttrs, SortbyClauseContextAttrs, StringLiteralContextAll,
    SuffixSortFieldContextAttrs, TableCommandContextAttrs, TableIdentContextAll, TableIdentContextAttrs,
    TableOrSubqueryClauseContextAttrs, TableQualifiedNameContextAll, TableSourceClauseContextAttrs,
    TableSourceContextAll, TableSourceContextAttrs, TimestampFunctionCallContextAttrs, ValueExprContextAttrs,
    ValueExpressionContextAll, ValueListContextAttrs, WcFieldListContextAttrs, WcQualifiedNameContextAll,
    WhereCommandContextAttrs, WildcardContextAll, WildcardContextAttrs, parse_ppl_root, ruleNames,
};

use super::ast::{
    ArithmeticOperator, CastTarget, CompareOperator, PplCommand, PplExpr, PplLiteral, PplQuery, PplSortKey, PplSource,
};
use super::error::PplError;

/// How many rows `head` keeps when it names no number.
const DEFAULT_HEAD_COUNT: usize = 10;

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
/// direction).
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
    let Some(search) = first_command.searchCommand() else {
        return Err(build_unsupported_command_error(&name_first_child_rule(first_command)));
    };
    let SearchCommandContextAll::SearchFromContext(search) = search.as_ref() else {
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
    let Some(sources) = clause.tableSourceClause() else {
        return Err(build_unsupported_command_error("source=[subsearch]"));
    };
    let names = sources
        .tableSource_all()
        .iter()
        .map(|source| name_table_source(source))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PplSource::Index {
        expression: names.join(","),
    })
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
    Err(build_unsupported_command_error(&name_first_child_rule(command)))
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

/// The dotted path of a wildcard-capable name. `*` inside an identifier is the wildcard; the
/// grammar's `%` wildcard form is refused, so a pattern means one thing.
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
    Ok(parts.join("."))
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
        ValueExpressionContextAll::ScalarSubqueryExprContext(_)
        | ValueExpressionContextAll::InSubqueryExprContext(_)
        | ValueExpressionContextAll::ExistsSubqueryExprContext(_) => Err(build_unsupported_command_error("subsearch")),
        ValueExpressionContextAll::Error(_) => Err(PplError::Internal {
            reason: "an unlabeled value expression".to_string(),
        }),
    }
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

/// The dotted path of a field reference, backticks removed.
fn name_field(field: &FieldExpressionContextAll<'_>) -> Result<String, PplError> {
    let name = field.qualifiedName().ok_or_else(|| build_missing_part_error("field"))?;
    join_qualified_name(&name)
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
            ("describe logs", "describe"),
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
        // `take` is an aggregation of `stats`, which this planner does not plan yet.
        assert_eq!(
            parse_ppl("source=logs | stats take(body, 5)"),
            Err(PplError::UnsupportedCommand {
                command: "stats".to_string()
            })
        );
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
}
