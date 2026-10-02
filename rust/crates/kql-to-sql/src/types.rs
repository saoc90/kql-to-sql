//! Kusto scalar type helpers.

use crate::KqlType;

impl KqlType {
    /// Parses a Kusto type name, including aliases (`double`, `boolean`, `date`, `time`, `uuid`).
    pub fn from_name(name: &str) -> Option<KqlType> {
        Some(match name.to_ascii_lowercase().as_str() {
            "bool" | "boolean" => KqlType::Bool,
            "int" | "int32" => KqlType::Int,
            "long" | "int64" => KqlType::Long,
            "real" | "double" => KqlType::Real,
            "decimal" => KqlType::Decimal,
            "string" => KqlType::String,
            "datetime" | "date" => KqlType::DateTime,
            "timespan" | "time" => KqlType::TimeSpan,
            "guid" | "uuid" | "uniqueid" => KqlType::Guid,
            "dynamic" => KqlType::Dynamic,
            _ => return None,
        })
    }

    pub fn is_numeric(self) -> bool {
        matches!(self, KqlType::Int | KqlType::Long | KqlType::Real | KqlType::Decimal)
    }

    pub fn is_integer(self) -> bool {
        matches!(self, KqlType::Int | KqlType::Long)
    }

    /// The .NET type name used by `getschema`'s `DataType` column.
    pub fn clr_name(self) -> &'static str {
        match self {
            KqlType::Bool => "System.SByte",
            KqlType::Int => "System.Int32",
            KqlType::Long => "System.Int64",
            KqlType::Real => "System.Double",
            KqlType::Decimal => "System.Data.SqlTypes.SqlDecimal",
            KqlType::String => "System.String",
            KqlType::DateTime => "System.DateTime",
            KqlType::TimeSpan => "System.TimeSpan",
            KqlType::Guid => "System.Guid",
            KqlType::Dynamic => "System.Object",
        }
    }

    /// `gettype()` result names.
    pub fn gettype_name(self) -> &'static str {
        match self {
            KqlType::Bool => "bool",
            KqlType::Int => "int",
            KqlType::Long => "long",
            KqlType::Real => "real",
            KqlType::Decimal => "decimal",
            KqlType::String => "string",
            KqlType::DateTime => "datetime",
            KqlType::TimeSpan => "timespan",
            KqlType::Guid => "guid",
            KqlType::Dynamic => "dynamic",
        }
    }
}

/// The wider of two numeric types (`int < long < decimal < real`), Kusto's `Widest` rule.
pub fn widest(a: KqlType, b: KqlType) -> KqlType {
    fn rank(t: KqlType) -> u8 {
        match t {
            KqlType::Int => 1,
            KqlType::Long => 2,
            KqlType::Decimal => 3,
            KqlType::Real => 4,
            _ => 0,
        }
    }
    if rank(a) >= rank(b) {
        a
    } else {
        b
    }
}

/// The common type of several values for `iff`/`case`/`coalesce`/`datatable` columns, if any.
pub fn common_type(types: &[KqlType]) -> Option<KqlType> {
    let first = *types.first()?;
    let mut t = first;
    for &u in &types[1..] {
        if u == t {
            continue;
        }
        if t.is_numeric() && u.is_numeric() {
            t = widest(t, u);
        } else {
            return None;
        }
    }
    Some(t)
}
