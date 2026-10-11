//! The fields an index exposes, as `_field_caps` and `_mapping` report them.

/// An `OpenSearch` field type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldType {
    /// Analyzed full text: searchable, not aggregatable.
    Text,
    /// An exact string.
    Keyword,
    /// A point in time.
    Date,
    /// A 32-bit integer.
    Integer,
    /// A 64-bit integer.
    Long,
    /// A floating-point or decimal number.
    Double,
    /// `true` or `false`.
    Boolean,
    /// A structured value: present in `_source`, not itself searchable or aggregatable.
    Object,
}

impl FieldType {
    /// The type's `OpenSearch` name.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Keyword => "keyword",
            Self::Date => "date",
            Self::Integer => "integer",
            Self::Long => "long",
            Self::Double => "double",
            Self::Boolean => "boolean",
            Self::Object => "object",
        }
    }
}

/// One field of an index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexField {
    /// The name clients use. For a MAP key it is `<column>.<stored key>`.
    pub(crate) name: String,
    /// The `OpenSearch` type.
    pub(crate) field_type: FieldType,
    /// Queries may filter on the field.
    pub(crate) is_searchable: bool,
    /// Aggregations and sorts may use the field.
    pub(crate) is_aggregatable: bool,
    /// The structural field this one sits under: the MAP column of a key. Dots inside a stored
    /// key are never structure.
    pub(crate) parent: Option<String>,
}

impl IndexField {
    /// A searchable scalar field, aggregatable unless it is `text`.
    pub(crate) fn leaf(name: String, field_type: FieldType, parent: Option<String>) -> Self {
        Self {
            name,
            is_searchable: true,
            is_aggregatable: field_type != FieldType::Text,
            field_type,
            parent,
        }
    }

    /// An `object` field: present in `_source`, neither searchable nor aggregatable itself.
    pub(crate) const fn object(name: String) -> Self {
        Self {
            name,
            field_type: FieldType::Object,
            is_searchable: false,
            is_aggregatable: false,
            parent: None,
        }
    }
}
