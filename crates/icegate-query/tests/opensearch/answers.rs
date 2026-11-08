//! Readers of `_search` answers shared by the data-index test modules. A reader answers an error
//! naming what the answer lacks, so a test `.expect`s it where the expectation is stated.

use std::error::Error;

use serde_json::Value;

/// The text `_source.<column>` of every hit, in hit order.
pub fn read_source_texts<'a>(answer: &'a Value, column: &str) -> Result<Vec<&'a str>, Box<dyn Error>> {
    answer["hits"]["hits"]
        .as_array()
        .ok_or_else(|| format!("no hits array in {answer}"))?
        .iter()
        .map(|hit| {
            hit["_source"][column]
                .as_str()
                .ok_or_else(|| format!("no text _source.{column} in {hit}").into())
        })
        .collect()
}

/// The same texts, sorted, for hit lists whose order is not a contract.
pub fn read_sorted_source_texts(answer: &Value, column: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let mut texts: Vec<String> = read_source_texts(answer, column)?.into_iter().map(str::to_string).collect();
    texts.sort_unstable();
    Ok(texts)
}
