//! Abstract syntax tree for KQL queries.
//!
//! The tree is purely syntactic: names are not resolved and nothing is typed. Binding and type
//! checking happen in the translator.

/// A byte range into the original query text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

/// A whole query: `let` statements followed by (usually one) tabular or scalar expression.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub statements: Vec<Statement>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Let {
        name: String,
        value: LetValue,
    },
    /// `set name [= value];` query options. Ignored by the translator.
    Set {
        name: String,
        value: Option<Expr>,
    },
    Expr(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LetValue {
    Expr(Expr),
    Function(Function),
}

/// A user-defined function (`let f = (a:int) { ... }`) or view.
#[derive(Debug, Clone, PartialEq)]
pub struct Function {
    pub params: Vec<Param>,
    pub body: Vec<Statement>,
    pub is_view: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Param {
    pub name: String,
    pub ty: ParamType,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParamType {
    Scalar(String),
    /// A tabular parameter: `T:(*)` (columns empty, open) or `T:(a:int, ...)`.
    Tabular {
        columns: Vec<ColumnDecl>,
        open: bool,
    },
}

/// `name:type` in `datatable`, tabular parameters, `externaldata` etc.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDecl {
    pub name: String,
    pub ty: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// `null` or a typed null such as `int(null)`; `ty` holds the type name if given.
    Null(Option<String>),
    Bool(bool),
    Int(i64),
    Long(i64),
    Real(f64),
    Decimal(String),
    String(String),
    /// Raw text of a `datetime(...)` literal, e.g. `2020-01-01 10:00`.
    DateTime(String),
    /// Timespan in ticks (100ns).
    TimeSpan(i64),
    Guid(String),
    Dynamic(Json),
}

/// A JSON value inside a `dynamic(...)` literal.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    /// Number kept as text so integers and reals round-trip exactly.
    Number(String),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
    /// Kusto allows typed scalars such as `datetime(...)` inside dynamic literals.
    Scalar(Box<Literal>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
    And,
    Or,
    /// `=~`
    EqTilde,
    /// `!~`
    NeTilde,
    /// String predicates such as `has`, `contains_cs`, `!startswith`.
    Str(StringOp, bool /* negated */),
    MatchesRegex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringOp {
    Has,
    HasCs,
    HasPrefix,
    HasPrefixCs,
    HasSuffix,
    HasSuffixCs,
    Contains,
    ContainsCs,
    StartsWith,
    StartsWithCs,
    EndsWith,
    EndsWithCs,
    Like,
    LikeCs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Plus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Literal),
    /// An identifier or bracketed name (`['a b']`).
    Name(String),
    /// `*` (as in `count(*)`, `arg_max(x, *)`, `distinct *`).
    Star,
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    /// `in`, `!in`, `in~`, `!in~`, `has_any`, `has_all`.
    In {
        kind: InKind,
        expr: Box<Expr>,
        list: Vec<Expr>,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Call {
        name: String,
        args: Vec<Arg>,
    },
    /// `expr.name`
    Member {
        expr: Box<Expr>,
        name: String,
    },
    /// `expr[index]`
    Index {
        expr: Box<Expr>,
        index: Box<Expr>,
    },
    /// `left | operator`
    Pipe {
        input: Box<Expr>,
        op: Box<Operator>,
    },
    /// A query that starts with a source operator (`print`, `datatable`, `range`, `union`, ...).
    Source(Box<Operator>),
    /// A parenthesized expression; kept so tabular sub-queries in argument lists are recognizable.
    Paren(Box<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InKind {
    In,
    NotIn,
    InCi,
    NotInCi,
    HasAny,
    HasAll,
}

/// A function argument, possibly named (`bin(x, 1h)` vs `bag_pack(name=...)` is rare but valid).
#[derive(Debug, Clone, PartialEq)]
pub struct Arg {
    pub name: Option<String>,
    pub expr: Expr,
}

impl Arg {
    pub fn positional(expr: Expr) -> Self {
        Arg { name: None, expr }
    }
}

/// `expr`, `name = expr` or `(a, b) = expr`.
#[derive(Debug, Clone, PartialEq)]
pub struct NamedExpr {
    pub names: Vec<String>,
    pub expr: Expr,
}

impl NamedExpr {
    pub fn name(&self) -> Option<&str> {
        self.names.first().map(String::as_str)
    }
}

/// A query operator parameter such as `kind=inner` or `hint.strategy=shuffle`.
#[derive(Debug, Clone, PartialEq)]
pub struct OpParam {
    pub name: String,
    pub value: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderKey {
    pub expr: Expr,
    /// `None` means the operator's default (desc for sort/top).
    pub dir: Option<SortDir>,
    pub nulls: Option<NullsOrder>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopNestedLevel {
    pub count: Option<Expr>,
    pub name: Option<String>,
    pub of: Expr,
    pub others: Option<Expr>,
    pub by_name: Option<String>,
    pub by: Expr,
    pub dir: Option<SortDir>,
    pub nulls: Option<NullsOrder>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsePart {
    /// A string constant to match.
    Text(String),
    /// `*`: skip anything.
    Star,
    /// A column to extract, with optional `:type`.
    Column { name: String, ty: Option<String> },
    /// A regex fragment (`kind=regex` only, given as a string literal).
    Regex(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MvExpandItem {
    pub name: Option<String>,
    pub expr: Expr,
    pub to_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MakeSeriesAgg {
    pub name: Option<String>,
    pub expr: Expr,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScanStep {
    pub name: String,
    pub optional: bool,
    pub condition: Expr,
    pub assignments: Vec<(String, Expr)>,
}

/// Name pattern used by `project-away`, `project-keep`, `project-reorder` (may contain `*`).
#[derive(Debug, Clone, PartialEq)]
pub struct NamePattern {
    pub pattern: String,
    pub dir: Option<SortDir>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    Where(Expr),
    Extend(Vec<NamedExpr>),
    Project(Vec<NamedExpr>),
    ProjectAway(Vec<NamePattern>),
    ProjectKeep(Vec<NamePattern>),
    ProjectRename(Vec<(String, String)>),
    ProjectReorder(Vec<NamePattern>),
    Summarize {
        params: Vec<OpParam>,
        aggs: Vec<NamedExpr>,
        by: Vec<NamedExpr>,
    },
    Sort(Vec<OrderKey>),
    Take(Expr),
    Top {
        count: Expr,
        key: OrderKey,
        params: Vec<OpParam>,
    },
    TopNested(Vec<TopNestedLevel>),
    TopHitters {
        count: Expr,
        of: Expr,
        by: Option<Expr>,
    },
    Count {
        name: Option<String>,
    },
    Distinct(Vec<Expr>),
    Join {
        params: Vec<OpParam>,
        right: Expr,
        on: Vec<Expr>,
    },
    Lookup {
        params: Vec<OpParam>,
        right: Expr,
        on: Vec<Expr>,
    },
    Union {
        params: Vec<OpParam>,
        tables: Vec<Expr>,
    },
    MvExpand {
        params: Vec<OpParam>,
        items: Vec<MvExpandItem>,
        limit: Option<Expr>,
    },
    MvApply {
        params: Vec<OpParam>,
        items: Vec<MvExpandItem>,
        limit: Option<Expr>,
        context_id: Option<String>,
        body: Vec<Operator>,
    },
    Parse {
        params: Vec<OpParam>,
        expr: Expr,
        parts: Vec<ParsePart>,
        filter: bool,
    },
    ParseKv {
        expr: Expr,
        columns: Vec<ColumnDecl>,
        params: Vec<OpParam>,
    },
    As {
        params: Vec<OpParam>,
        name: String,
    },
    Serialize(Vec<NamedExpr>),
    Sample(Expr),
    SampleDistinct {
        count: Expr,
        of: Expr,
    },
    Search {
        params: Vec<OpParam>,
        tables: Vec<Expr>,
        predicate: Expr,
    },
    MakeSeries {
        params: Vec<OpParam>,
        aggs: Vec<MakeSeriesAgg>,
        on: Expr,
        from: Option<Expr>,
        to: Option<Expr>,
        step: Expr,
        by: Vec<NamedExpr>,
    },
    Scan {
        order_by: Vec<OrderKey>,
        partition_by: Vec<Expr>,
        declare: Vec<(String, String, Option<Expr>)>,
        steps: Vec<ScanStep>,
    },
    Evaluate {
        params: Vec<OpParam>,
        name: String,
        args: Vec<Arg>,
    },
    Invoke {
        name: String,
        args: Vec<Arg>,
    },
    Render {
        chart: String,
        props: Vec<(String, Expr)>,
    },
    GetSchema,
    Consume,
    Fork(Vec<(Option<String>, Vec<Operator>)>),
    Facet {
        by: Vec<String>,
        with: Option<Vec<Operator>>,
    },
    Partition {
        params: Vec<OpParam>,
        by: Expr,
        body: Vec<Operator>,
    },
    Reduce {
        by: Expr,
        params: Vec<OpParam>,
    },
    // ----- source operators -----
    Print(Vec<NamedExpr>),
    DataTable {
        columns: Vec<ColumnDecl>,
        values: Vec<Expr>,
    },
    Range {
        name: String,
        from: Expr,
        to: Expr,
        step: Expr,
    },
    ExternalData {
        columns: Vec<ColumnDecl>,
        uris: Vec<Expr>,
        props: Vec<(String, Expr)>,
    },
    Find {
        tables: Vec<Expr>,
        predicate: Expr,
    },
}

impl Operator {
    /// The operator keyword as written in KQL.
    pub fn keyword(&self) -> &'static str {
        match self {
            Operator::Where(_) => "where",
            Operator::Extend(_) => "extend",
            Operator::Project(_) => "project",
            Operator::ProjectAway(_) => "project-away",
            Operator::ProjectKeep(_) => "project-keep",
            Operator::ProjectRename(_) => "project-rename",
            Operator::ProjectReorder(_) => "project-reorder",
            Operator::Summarize { .. } => "summarize",
            Operator::Sort(_) => "sort",
            Operator::Take(_) => "take",
            Operator::Top { .. } => "top",
            Operator::TopNested(_) => "top-nested",
            Operator::TopHitters { .. } => "top-hitters",
            Operator::Count { .. } => "count",
            Operator::Distinct(_) => "distinct",
            Operator::Join { .. } => "join",
            Operator::Lookup { .. } => "lookup",
            Operator::Union { .. } => "union",
            Operator::MvExpand { .. } => "mv-expand",
            Operator::MvApply { .. } => "mv-apply",
            Operator::Parse { filter: false, .. } => "parse",
            Operator::Parse { filter: true, .. } => "parse-where",
            Operator::ParseKv { .. } => "parse-kv",
            Operator::As { .. } => "as",
            Operator::Serialize(_) => "serialize",
            Operator::Sample(_) => "sample",
            Operator::SampleDistinct { .. } => "sample-distinct",
            Operator::Search { .. } => "search",
            Operator::MakeSeries { .. } => "make-series",
            Operator::Scan { .. } => "scan",
            Operator::Evaluate { .. } => "evaluate",
            Operator::Invoke { .. } => "invoke",
            Operator::Render { .. } => "render",
            Operator::GetSchema => "getschema",
            Operator::Consume => "consume",
            Operator::Fork(_) => "fork",
            Operator::Facet { .. } => "facet",
            Operator::Partition { .. } => "partition",
            Operator::Reduce { .. } => "reduce",
            Operator::Print(_) => "print",
            Operator::DataTable { .. } => "datatable",
            Operator::Range { .. } => "range",
            Operator::ExternalData { .. } => "externaldata",
            Operator::Find { .. } => "find",
        }
    }
}
