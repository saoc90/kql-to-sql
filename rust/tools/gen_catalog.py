#!/usr/bin/env python3
"""Generate crates/kql-to-sql/src/catalog_data.rs from Microsoft's Kusto.Language C# sources.

Kusto.Language (https://github.com/microsoft/Kusto-Query-Language) is Apache-2.0 licensed.

Usage:
    python3 tools/gen_catalog.py [--src PATH/TO/src/Kusto.Language] [--out PATH/TO/catalog_data.rs]

Defaults: --src $KUSTO_LANGUAGE_SRC or /home/user/microsoft/kusto-query-language/src/Kusto.Language,
          --out <this script>/../crates/kql-to-sql/src/catalog_data.rs

The script contains a tiny tokenizer + recursive-descent parser for the subset of C# used by the
`static readonly FunctionSymbol X = new FunctionSymbol(...)...;` declarations in Functions.cs,
Functions.Convert.cs and Aggregates.cs, and then "evaluates" those expressions symbolically,
mirroring the constructors in Symbols/FunctionSymbol.cs, Symbols/Signature.cs and Symbols/Parameter.cs.
Only symbols listed in each class's `All` array are emitted.
"""

import argparse
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SRC = os.environ.get(
    "KUSTO_LANGUAGE_SRC", "/home/user/microsoft/kusto-query-language/src/Kusto.Language"
)
DEFAULT_OUT = os.path.join(HERE, "..", "crates", "kql-to-sql", "src", "catalog_data.rs")

# FunctionHelpers.MaxRepeat
MAX_REPEAT = 32767

# ---------------------------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------------------------

TOKEN_RE = re.compile(
    r"""
    (?P<ws>\s+)
  | (?P<lcomment>//[^\n]*|\#[^\n]*)
  | (?P<bcomment>/\*.*?\*/)
  | (?P<vstr>[$]?@"(?:[^"]|"")*")
  | (?P<str>[$]?"(?:[^"\\\n]|\\.)*")
  | (?P<char>'(?:[^'\\\n]|\\.)+')
  | (?P<num>\d+(?:\.\d+)?(?:[eE][+-]?\d+)?[uUlLfFdDmM]*|0[xX][0-9a-fA-F]+)
  | (?P<ident>@?[A-Za-z_][A-Za-z_0-9]*)
  | (?P<op>=>|==|!=|<=|>=|&&|\|\||\?\?|\?\.|\+\+|--|[{}()\[\];,.:?=<>+\-*/%!&|^~])
    """,
    re.S | re.X,
)


class Tok:
    __slots__ = ("kind", "text", "pos")

    def __init__(self, kind, text, pos):
        self.kind, self.text, self.pos = kind, text, pos

    def __repr__(self):
        return f"{self.kind}:{self.text}"


def tokenize(src):
    toks = []
    i, n = 0, len(src)
    while i < n:
        m = TOKEN_RE.match(src, i)
        if not m:
            raise SyntaxError(f"cannot tokenize at offset {i}: {src[i:i+40]!r}")
        kind = m.lastgroup
        if kind not in ("ws", "lcomment", "bcomment"):
            text = m.group(kind)
            if kind == "vstr":
                body = text[text.index('"') + 1 : -1].replace('""', '"')
                toks.append(Tok("str", body, i))
            elif kind == "str":
                body = text[text.index('"') + 1 : -1]
                body = re.sub(r"\\(.)", lambda mm: {"n": "\n", "t": "\t"}.get(mm.group(1), mm.group(1)), body)
                toks.append(Tok("str", body, i))
            else:
                toks.append(Tok(kind, text, i))
        i = m.end()
    toks.append(Tok("eof", "", n))
    return toks


# ---------------------------------------------------------------------------------------------
# Expression parser (subset of C#)
# ---------------------------------------------------------------------------------------------
# AST nodes are tuples:
#   ('new', typename, args, init)    args: [(argname|None, node)], init: [node] | None
#   ('array', [node])                new[] {..} / new T[] {..}
#   ('name', 'A.B.C')
#   ('invoke', 'A.B', args)          static method call on a dotted name
#   ('call', target, 'Method', args) postfix instance method call
#   ('member', target, 'Name')
#   ('str', s) ('num', n) ('bool', b) ('null',)
#   ('lambda',)                      lambda bodies are skipped
#   ('binop', op, l, r)


class Parser:
    def __init__(self, toks, i=0):
        self.toks, self.i = toks, i

    def peek(self, k=0):
        return self.toks[self.i + k]

    def next(self):
        t = self.toks[self.i]
        self.i += 1
        return t

    def accept(self, text):
        if self.peek().text == text and self.peek().kind in ("op", "ident"):
            self.i += 1
            return True
        return False

    def expect(self, text):
        t = self.next()
        if t.text != text:
            raise SyntaxError(f"expected {text!r} got {t.text!r} at {t.pos}")
        return t

    def skip_balanced_until_arg_end(self):
        depth = 0
        while True:
            t = self.peek()
            if t.kind == "eof":
                raise SyntaxError("eof in lambda")
            if t.kind == "op":
                if t.text in "([{":
                    depth += 1
                elif t.text in ")]}":
                    if depth == 0:
                        return
                    depth -= 1
                elif t.text in (",", ";") and depth == 0:
                    return
            self.i += 1

    def matching_close(self, i):
        """Index of the token closing the bracket at index i."""
        pairs = {"(": ")", "[": "]", "{": "}"}
        depth = 0
        while True:
            t = self.toks[i]
            if t.kind == "eof":
                raise SyntaxError("unbalanced")
            if t.kind == "op" and t.text in pairs:
                depth += 1
            elif t.kind == "op" and t.text in ")]}":
                depth -= 1
                if depth == 0:
                    return i
            i += 1

    def is_lambda_start(self):
        t = self.peek()
        if t.kind == "ident" and self.peek(1).text == "=>":
            return True
        if t.kind == "op" and t.text == "(":
            j = self.matching_close(self.i)
            return self.toks[j + 1].text == "=>"
        return False

    def parse_expr(self):
        if self.is_lambda_start():
            self.skip_balanced_until_arg_end()
            return ("lambda",)
        left = self.parse_postfix()
        while self.peek().kind == "op" and self.peek().text in ("+", "-", "*", "/", "|", "&", "??"):
            op = self.next().text
            right = self.parse_postfix()
            left = ("binop", op, left, right)
        return left

    def parse_args(self, close=")"):
        args = []
        if self.accept(close):
            return args
        while True:
            name = None
            if self.peek().kind == "ident" and self.peek(1).text == ":":
                name = self.next().text
                self.next()
            args.append((name, self.parse_expr()))
            if self.accept(","):
                if self.peek().text == close:  # trailing comma
                    self.next()
                    return args
                continue
            self.expect(close)
            return args

    def parse_type_name(self):
        parts = [self.next().text]
        while self.peek().text == "." and self.peek(1).kind == "ident":
            self.next()
            parts.append(self.next().text)
        name = ".".join(parts)
        if self.peek().text == "<":
            j = self.i
            depth = 0
            while True:
                t = self.toks[j]
                if t.text == "<":
                    depth += 1
                elif t.text == ">":
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            name += "".join(t.text for t in self.toks[self.i : j + 1])
            self.i = j + 1
        return name

    def parse_primary(self):
        t = self.peek()
        if t.kind == "ident" and t.text == "new":
            self.next()
            if self.peek().text == "[":
                self.expect("[")
                self.expect("]")
                self.expect("{")
                return ("array", [e for _, e in self.parse_args("}")])
            tname = self.parse_type_name()
            if self.peek().text == "[":
                self.expect("[")
                self.expect("]")
                self.expect("{")
                return ("array", [e for _, e in self.parse_args("}")])
            args = []
            if self.accept("("):
                args = self.parse_args(")")
            init = None
            if self.peek().text == "{":
                self.next()
                init = [e for _, e in self.parse_args("}")]
            return ("new", tname, args, init)
        if t.kind == "op" and t.text == "(":
            self.next()
            e = self.parse_expr()
            self.expect(")")
            return e
        if t.kind == "op" and t.text == "-" and self.peek(1).kind == "num":
            self.next()
            return ("num", -parse_num(self.next().text))
        if t.kind == "str":
            self.next()
            return ("str", t.text)
        if t.kind == "num":
            self.next()
            return ("num", parse_num(t.text))
        if t.kind == "ident":
            if t.text in ("true", "false"):
                self.next()
                return ("bool", t.text == "true")
            if t.text == "null":
                self.next()
                return ("null",)
            if t.text in ("typeof", "nameof"):
                self.next()
                self.expect("(")
                self.skip_balanced_until_arg_end()
                self.expect(")")
                return ("name", t.text)
            parts = [self.next().text]
            while self.peek().text == "." and self.peek(1).kind == "ident" and self.peek(2).text != "(":
                self.next()
                parts.append(self.next().text)
            name = ".".join(parts)
            if self.peek().text == "(":
                self.next()
                return ("invoke", name, self.parse_args(")"))
            return ("name", name)
        raise SyntaxError(f"unexpected token {t.text!r} at {t.pos}")

    def parse_postfix(self):
        e = self.parse_primary()
        while True:
            if self.peek().text in (".", "?.") and self.peek(1).kind == "ident":
                self.next()
                m = self.next().text
                if self.peek().text == "(":
                    self.next()
                    e = ("call", e, m, self.parse_args(")"))
                else:
                    e = ("member", e, m)
            elif self.peek().text == "!" and self.peek(1).text in (".", ")", ",", ";"):
                self.next()  # null-forgiving
            else:
                return e


def parse_num(text):
    text = text.rstrip("uUlLfFdDmM")
    if text.lower().startswith("0x"):
        return int(text, 16)
    return float(text) if ("." in text or "e" in text.lower()) else int(text)


# ---------------------------------------------------------------------------------------------
# Field extraction
# ---------------------------------------------------------------------------------------------

FIELD_TYPES = {"FunctionSymbol", "Parameter", "TypeSymbol", "CustomReturnType", "string[]", "Signature",
               "ScalarSymbol", "TupleSymbol", "DynamicBagSymbol", "DynamicArraySymbol"}


def extract_fields(toks, cls):
    """Find `static [readonly] [new] TYPE NAME = expr;` fields. Returns {qualified name: (type, ast)}."""
    fields = {}
    alls = {}
    i = 0
    n = len(toks)
    while i < n:
        t = toks[i]
        if t.kind == "ident" and t.text == "static":
            j = i + 1
            while toks[j].text in ("readonly", "new"):
                j += 1
            # type name (possibly generic / array)
            k = j
            tname = toks[k].text
            k += 1
            if toks[k].text == "[" and toks[k + 1].text == "]":
                tname += "[]"
                k += 2
            if toks[k].text == "<":
                depth = 0
                while True:
                    if toks[k].text == "<":
                        depth += 1
                    elif toks[k].text == ">":
                        depth -= 1
                        if depth == 0:
                            break
                    tname += toks[k].text
                    k += 1
                tname += ">"
                k += 1
            if toks[k].kind == "ident":
                fname = toks[k].text
                k += 1
                if fname == "All" and toks[k].text == "{":
                    # public static IReadOnlyList<FunctionSymbol> All { get; } = new FunctionSymbol[] {...};
                    while toks[k].text != "=":
                        k += 1
                    p = Parser(toks, k + 1)
                    alls[cls] = p.parse_expr()
                    i = p.i
                    continue
                if toks[k].text == "=" and tname in FIELD_TYPES:
                    p = Parser(toks, k + 1)
                    try:
                        ast = p.parse_expr()
                        if p.peek().text != ";":
                            raise SyntaxError(f"expected ';' after {fname}, got {p.peek().text!r}")
                        fields[f"{cls}.{fname}"] = (tname, ast)
                    except SyntaxError as ex:
                        fields[f"{cls}.{fname}"] = (tname, ("error", str(ex)))
                    i = p.i
                    continue
        i += 1
    return fields, alls


# ---------------------------------------------------------------------------------------------
# Symbolic evaluation
# ---------------------------------------------------------------------------------------------

SCALAR_TYPE_NAMES = {
    "Bool": "bool", "Boolean": "bool",
    "Int": "int", "Long": "long", "Real": "real", "Decimal": "decimal",
    "String": "string", "DateTime": "datetime", "TimeSpan": "timespan", "Guid": "guid",
    "Dynamic": "dynamic", "Type": "type", "Unknown": "unknown",
}

RETURN_TYPE_KINDS = []  # filled from ReturnTypeKind.cs
RESULT_NAME_KINDS = []  # filled from ResultNameKind.cs
PARAMETER_TYPE_KINDS = []  # filled from ParameterTypeKind.cs


class EvalError(Exception):
    pass


class Evaluator:
    def __init__(self, fields):
        self.fields = fields

    def resolve(self, name, cls):
        for cand in (name, f"{cls}.{name}"):
            if cand in self.fields:
                return cand
        return None

    def field_ast(self, node, cls):
        if node[0] == "name":
            q = self.resolve(node[1], cls)
            if q:
                tname, ast = self.fields[q]
                if ast[0] == "error":
                    raise EvalError(ast[1])
                return tname, ast, q.split(".")[0]
        return None

    # ---- types ----
    def type_of(self, node, cls):
        """Return a Kusto type name string if node is a TypeSymbol expression, else None."""
        k = node[0]
        if k == "name":
            nm = node[1]
            if nm.startswith("ScalarTypes."):
                s = nm[len("ScalarTypes."):]
                if s in SCALAR_TYPE_NAMES:
                    return SCALAR_TYPE_NAMES[s]
                if s.startswith("Dynamic") or s == "GeoShape":
                    return "dynamic"
                raise EvalError(f"unknown scalar type {nm}")
            f = self.field_ast(node, cls)
            if f and f[0] in ("TypeSymbol", "ScalarSymbol", "TupleSymbol", "DynamicBagSymbol", "DynamicArraySymbol"):
                return self.type_of(f[1], f[2])
            return None
        if k == "invoke" and node[1].startswith("ScalarTypes.GetDynamic"):
            return "dynamic"
        if k == "call" and node[1] == ("name", "ScalarTypes") and node[2].startswith("GetDynamic"):
            return "dynamic"
        if k == "new" and node[1] in ("DynamicBagSymbol", "DynamicArraySymbol"):
            return "dynamic"
        if k == "new" and node[1] == "TupleSymbol":
            return "dynamic"
        if k == "member" and node[2] in ("Empty",):
            return None
        return None

    # ---- parameters ----
    def is_parameter(self, node, cls):
        if node[0] == "new" and node[1] == "Parameter":
            return True
        f = self.field_ast(node, cls)
        return bool(f and f[0] == "Parameter")

    def param(self, node, cls):
        f = self.field_ast(node, cls)
        if f and f[0] == "Parameter":
            return self.param(f[1], f[2])
        if not (node[0] == "new" and node[1] == "Parameter"):
            raise EvalError(f"not a parameter: {node!r}")
        args = node[2]
        positional_names = ["name", "type", "argumentKind", "values", "examples", "isCaseSensitive",
                            "defaultValueIndicator", "minOccurring", "maxOccurring", "defaultValue", "description"]
        vals = {}
        pos = 0
        for an, av in args:
            if an is None:
                if pos >= len(positional_names):
                    raise EvalError("too many parameter args")
                vals[positional_names[pos]] = av
                pos += 1
            else:
                key = "type" if an in ("typeKind", "types") else an
                vals[key] = av
        if "type" not in vals:
            raise EvalError("parameter without type")
        tnode = vals["type"]
        if tnode[0] == "name" and tnode[1].startswith("ParameterTypeKind."):
            ptype = tnode[1].split(".", 1)[1]
            if PARAMETER_TYPE_KINDS and ptype not in PARAMETER_TYPE_KINDS:
                raise EvalError(f"unknown ParameterTypeKind {ptype}")
        elif tnode[0] == "array":
            ts = []
            for e in tnode[1]:
                t = self.type_of(e, cls)
                if t is None:
                    raise EvalError(f"unknown parameter type {e!r}")
                if t not in ts:
                    ts.append(t)
            ptype = "|".join(ts)
        else:
            ptype = self.type_of(tnode, cls)
            if ptype is None:
                raise EvalError(f"unknown parameter type {tnode!r}")
        mn = self.num(vals.get("minOccurring", ("num", 1)), cls)
        mx = self.num(vals.get("maxOccurring", ("num", 1)), cls)
        dv = vals.get("defaultValue")
        if dv is not None and dv[0] != "null":
            mn, mx = 0, 1
        return {"type": ptype, "min": mn, "max": mx}

    def num(self, node, cls):
        k = node[0]
        if k == "num":
            return node[1]
        if k == "name":
            if node[1] in ("MaxRepeat", "FunctionHelpers.MaxRepeat"):
                return MAX_REPEAT
            if node[1] in ("int.MaxValue", "Int32.MaxValue"):
                return 2**31 - 1
            if node[1] in ("short.MaxValue", "Int16.MaxValue"):
                return 32767
        if k == "binop" and node[1] in ("+", "-", "*"):
            a, b = self.num(node[2], cls), self.num(node[3], cls)
            return a + b if node[1] == "+" else a - b if node[1] == "-" else a * b
        raise EvalError(f"not a number: {node!r}")

    def flatten_params(self, args, cls):
        out = []
        for an, av in args:
            if an in ("description",):
                continue
            if av[0] == "array":
                for e in av[1]:
                    out.append(self.param(e, cls))
            elif av[0] == "new" and av[1].startswith("List<") and av[3] is not None:
                for e in av[3]:
                    out.append(self.param(e, cls))
            else:
                out.append(self.param(av, cls))
        return out

    # ---- signatures ----
    def is_signature(self, node, cls):
        if node[0] == "new" and node[1] == "Signature":
            return True
        if node[0] == "call":
            return self.is_signature(node[1], cls)
        if node[0] == "array":
            return bool(node[1]) and all(self.is_signature(e, cls) for e in node[1])
        f = self.field_ast(node, cls)
        return bool(f and f[0] == "Signature")

    def build_sig(self, args, cls):
        """args: constructor args after the function name (for FunctionSymbol) or all (for Signature)."""
        args = [(an, av) for an, av in args if an != "description"]
        if not args:
            raise EvalError("signature without return type")
        first = args[0][1]
        rest = args[1:]
        if first[0] == "name" and first[1].startswith("ReturnTypeKind."):
            rk = first[1].split(".", 1)[1]
            if rk not in RETURN_TYPE_KINDS:
                raise EvalError(f"unknown ReturnTypeKind {rk}")
            ret = ("kind", rk)
        elif len(rest) >= 1 and rest[0][1][0] == "name" and rest[0][1][1].startswith("Tabularity."):
            # (CustomReturnType, Tabularity, params) or (string body, Tabularity, params)
            ret = ("kind", "Computed") if first[0] == "str" else ("kind", "Custom")
            rest = rest[1:]
        elif first[0] == "str":
            ret = ("kind", "Computed")
        elif first[0] == "lambda":
            ret = ("kind", "Custom")
        else:
            t = self.type_of(first, cls)
            if t is not None:
                ret = ("fixed", t)
            else:
                f = self.field_ast(first, cls)
                if f and f[0] == "CustomReturnType":
                    ret = ("kind", "Custom")
                else:
                    raise EvalError(f"cannot determine return type from {first!r}")
        params = self.flatten_params(rest, cls)
        mn = sum(p["min"] for p in params)
        mx = sum(p["max"] for p in params)
        return {"ret": ret, "params": params, "min": mn, "max": mx, "hidden": False, "obsolete": False}

    def signature(self, node, cls):
        if node[0] == "call":
            s = self.signature(node[1], cls)
            m = node[2]
            if m == "Hide":
                s["hidden"] = True
            elif m == "WithIsHidden":
                s["hidden"] = self.boolean(node[3][0][1])
            elif m in ("Obsolete", "WithAlternative"):
                s["obsolete"] = True
            elif m in ("WithLayout",):
                pass
            else:
                raise EvalError(f"unknown Signature method {m}")
            return s
        f = self.field_ast(node, cls)
        if f and f[0] == "Signature":
            return self.signature(f[1], f[2])
        if node[0] == "new" and node[1] == "Signature":
            return self.build_sig(node[2], cls)
        raise EvalError(f"not a signature: {node!r}")

    def boolean(self, node):
        if node[0] == "bool":
            return node[1]
        raise EvalError(f"not a bool: {node!r}")

    def string(self, node):
        if node[0] == "str":
            return node[1]
        if node[0] == "null":
            return None
        raise EvalError(f"not a string literal: {node!r}")

    # ---- function symbols ----
    def function(self, node, cls):
        if node[0] == "call":
            fn = self.function(node[1], cls)
            m, args = node[2], node[3]
            if m == "WithResultNameKind":
                v = args[0][1]
                if v[0] != "name" or not v[1].startswith("ResultNameKind."):
                    raise EvalError(f"bad result name kind {v!r}")
                rk = v[1].split(".", 1)[1]
                if rk == "Default":
                    rk = "None"
                fn["result_name"] = rk
            elif m == "WithResultNamePrefix":
                fn["prefix"] = self.string(args[0][1])
            elif m == "Obsolete":
                fn["obsolete"] = True
                fn["hidden"] = True
            elif m == "WithIsObsolete":
                fn["obsolete"] = self.boolean(args[0][1])
            elif m == "Hide":
                fn["hidden"] = True
            elif m == "WithIsHidden":
                fn["hidden"] = self.boolean(args[0][1])
            elif m in ("ConstantFoldable", "WithIsConstantFoldable", "WithDescription", "WithCustomAvailability",
                       "WithOptimizedAlternative", "WithIsView"):
                pass
            else:
                raise EvalError(f"unknown FunctionSymbol method {m}")
            return fn
        f = self.field_ast(node, cls)
        if f and f[0] == "FunctionSymbol":
            return self.function(f[1], f[2])
        if not (node[0] == "new" and node[1] == "FunctionSymbol"):
            raise EvalError(f"not a FunctionSymbol construction: {node[0]}")
        args = node[2]
        name = self.string(args[0][1])
        rest = [(an, av) for an, av in args[1:] if an != "description"]
        if rest and all(self.is_signature(av, cls) for _, av in rest):
            sigs = []
            for _, av in rest:
                if av[0] == "array":
                    sigs.extend(self.signature(e, cls) for e in av[1])
                else:
                    sigs.append(self.signature(av, cls))
        elif len(rest) >= 1 and rest[0][1][0] == "str" and (len(rest) == 1 or rest[1][1][0] == "str"):
            # (name, parameterList, body) / (name, body) style declared functions
            sigs = [{"ret": ("kind", "Computed"), "params": [], "min": 0, "max": 0, "hidden": False, "obsolete": False}]
        else:
            sigs = [self.build_sig(rest, cls)]
        return {"name": name, "result_name": "None", "prefix": None, "signatures": sigs,
                "obsolete": False, "hidden": False}


# ---------------------------------------------------------------------------------------------
# Enum parsing
# ---------------------------------------------------------------------------------------------


def parse_enum(path, enum_name):
    src = open(path, encoding="utf-8-sig").read()
    m = re.search(r"enum\s+" + enum_name + r"\s*\{(.*?)\}", src, re.S)
    body = m.group(1)
    variants = []  # (name, doc)
    doc = []
    for line in body.splitlines():
        s = line.strip()
        if s.startswith("///"):
            txt = re.sub(r"</?summary>|</?remarks>", "", s[3:]).strip()
            txt = re.sub(r'<see cref="([^"]+)"\s*/>', r"\1", txt)
            if txt:
                doc.append(txt)
            continue
        mm = re.match(r"([A-Za-z_][A-Za-z_0-9]*)\s*(=\s*[A-Za-z_0-9]+)?\s*,?", s)
        if mm:
            if mm.group(2) is None:
                variants.append((mm.group(1), " ".join(doc)))
            doc = []
    return variants


# ---------------------------------------------------------------------------------------------
# Rust emission
# ---------------------------------------------------------------------------------------------


def rs_str(s):
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def emit(entries, ret_kinds, out_path, src_dir):
    L = []
    w = L.append
    w("// @generated by tools/gen_catalog.py from Kusto.Language (Apache-2.0). Do not edit by hand.")
    w("#![allow(dead_code)]")
    w("")
    w("#[derive(Debug, Clone, Copy, PartialEq, Eq)]")
    w("pub enum FnKind { Scalar, Aggregate }")
    w("")
    w("/// Return type rule, mirroring Kusto.Language's ReturnTypeKind.")
    w("#[derive(Debug, Clone, Copy, PartialEq, Eq)]")
    w("pub enum Ret {")
    w('    /// A fixed type, by Kusto type name: "bool","int","long","real","decimal","string","datetime","timespan","guid","dynamic".')
    w("    /// (This is ReturnTypeKind.Declared with its declared type.)")
    w("    Fixed(&'static str),")
    for name, doc in ret_kinds:
        if doc:
            w(f"    /// {doc}")
        w(f"    {name},")
    w("}")
    w("")
    w("#[derive(Debug, Clone, Copy, PartialEq, Eq)]")
    w("pub enum ResultName { " + ", ".join(n for n, _ in RESULT_NAME_KINDS_FULL) + " }")
    w("")
    w("#[derive(Debug, Clone, Copy)]")
    w("pub struct Sig {")
    w("    pub ret: Ret,")
    w('    /// Parameter type kinds / types as written, e.g. "long", "Summable", "Scalar", "StringOrDynamic", "Integer", "DynamicArray", "Any".')
    w("    pub params: &'static [&'static str],")
    w("    pub min_args: u8,")
    w("    /// u8::MAX for unbounded (repeatable parameters).")
    w("    pub max_args: u8,")
    w("}")
    w("")
    w("#[derive(Debug, Clone, Copy)]")
    w("pub struct FnInfo {")
    w("    pub name: &'static str,")
    w("    pub kind: FnKind,")
    w("    pub result_name: ResultName,")
    w("    /// WithResultNamePrefix value if any.")
    w("    pub prefix: Option<&'static str>,")
    w("    pub signatures: &'static [Sig],")
    w("    pub obsolete: bool,")
    w("    pub hidden: bool,")
    w("}")
    w("")
    w("pub static FUNCTIONS: &[FnInfo] = &[")
    for e in entries:
        w("    FnInfo {")
        w(f"        name: {rs_str(e['name'])},")
        w(f"        kind: FnKind::{e['kind']},")
        w(f"        result_name: ResultName::{e['result_name']},")
        w(f"        prefix: {('Some(' + rs_str(e['prefix']) + ')') if e['prefix'] is not None else 'None'},")
        w("        signatures: &[")
        for s in e["signatures"]:
            r = s["ret"]
            ret = f"Ret::Fixed({rs_str(r[1])})" if r[0] == "fixed" else f"Ret::{r[1]}"
            params = ", ".join(rs_str(p["type"]) for p in s["params"])
            mn = min(s["min"], 254)
            mx = 255 if s["max"] > 254 else s["max"]
            note = ""
            if s["hidden"] or s["obsolete"]:
                note = " // signature" + (" hidden" if s["hidden"] else "") + (" obsolete" if s["obsolete"] else "")
            w(f"            Sig {{ ret: {ret}, params: &[{params}], min_args: {mn}, max_args: {mx} }},{note}")
        w("        ],")
        w(f"        obsolete: {'true' if e['obsolete'] else 'false'},")
        w(f"        hidden: {'true' if e['hidden'] else 'false'},")
        w("    },")
    w("];")
    w("")
    w("/// Look up a function by exact (case-sensitive) name and kind.")
    w("pub fn lookup(name: &str, kind: FnKind) -> Option<&'static FnInfo> {")
    w("    let start = FUNCTIONS.partition_point(|f| f.name < name);")
    w("    FUNCTIONS[start..]")
    w("        .iter()")
    w("        .take_while(|f| f.name == name)")
    w("        .find(|f| f.kind == kind)")
    w("}")
    w("")
    w("#[cfg(test)]")
    w("mod tests {")
    w("    use super::*;")
    w("")
    w("    #[test]")
    w("    fn sorted() {")
    w("        for w in FUNCTIONS.windows(2) {")
    w("            assert!(w[0].name <= w[1].name, \"{} > {}\", w[0].name, w[1].name);")
    w("        }")
    w("    }")
    w("")
    w("    #[test]")
    w("    fn sum_and_count() {")
    w("        let sum = lookup(\"sum\", FnKind::Aggregate).unwrap();")
    w("        assert_eq!(sum.kind, FnKind::Aggregate);")
    w("        assert_eq!(sum.result_name, ResultName::PrefixAndFirstArgument);")
    w("        assert_eq!(sum.prefix, Some(\"sum\"));")
    w("        assert_eq!(sum.signatures[0].ret, Ret::Parameter0Promoted);")
    w("        assert_eq!(sum.signatures[0].params, &[\"Summable\"]);")
    w("        let count = lookup(\"count\", FnKind::Aggregate).unwrap();")
    w("        assert_eq!(count.prefix, Some(\"count\"));")
    w("        assert_eq!(count.signatures[0].ret, Ret::Fixed(\"long\"));")
    w("        assert_eq!(count.signatures[0].max_args, 0);")
    w("        assert!(lookup(\"sum\", FnKind::Scalar).is_none());")
    w("    }")
    w("")
    w("    #[test]")
    w("    fn scalars() {")
    w("        let bin = lookup(\"bin\", FnKind::Scalar).unwrap();")
    w("        assert_eq!(bin.kind, FnKind::Scalar);")
    w("        assert_eq!(bin.result_name, ResultName::FirstArgument);")
    w("        assert_eq!(bin.signatures[0].ret, Ret::Widest);")
    w("        assert_eq!(lookup(\"strlen\", FnKind::Scalar).unwrap().signatures[0].ret, Ret::Fixed(\"long\"));")
    w("        assert_eq!(lookup(\"tostring\", FnKind::Scalar).unwrap().signatures[0].ret, Ret::Fixed(\"string\"));")
    w("        let strcat = lookup(\"strcat\", FnKind::Scalar).unwrap();")
    w("        assert_eq!((strcat.signatures[0].min_args, strcat.signatures[0].max_args), (1, 64));")
    w("        let case = lookup(\"case\", FnKind::Scalar).unwrap();")
    w("        assert_eq!(case.signatures[0].max_args, u8::MAX);")
    w("        assert!(lookup(\"todouble\", FnKind::Scalar).is_some());")
    w("        assert!(lookup(\"toreal\", FnKind::Scalar).is_some());")
    w("        assert!(lookup(\"no_such_function\", FnKind::Scalar).is_none());")
    w("    }")
    w("")
    w("    #[test]")
    w("    fn obsolete_and_both_kinds() {")
    w("        let ms = lookup(\"makeset\", FnKind::Aggregate).unwrap();")
    w("        assert!(ms.obsolete && ms.hidden);")
    w("        assert!(!lookup(\"make_set\", FnKind::Aggregate).unwrap().obsolete);")
    w("        // `any` exists both as a graph scalar function and as an aggregate.")
    w("        assert!(lookup(\"any\", FnKind::Aggregate).is_some());")
    w("        assert!(lookup(\"any\", FnKind::Scalar).is_some());")
    w("        assert_eq!(lookup(\"arg_max\", FnKind::Aggregate).unwrap().signatures[0].ret, Ret::Custom);")
    w("    }")
    w("}")
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")


# ---------------------------------------------------------------------------------------------


RESULT_NAME_KINDS_FULL = []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default=DEFAULT_SRC)
    ap.add_argument("--out", default=DEFAULT_OUT)
    a = ap.parse_args()
    src = a.src

    ret_kinds = parse_enum(os.path.join(src, "Symbols", "ReturnTypeKind.cs"), "ReturnTypeKind")
    RETURN_TYPE_KINDS.extend(n for n, _ in ret_kinds)
    RESULT_NAME_KINDS_FULL.extend(parse_enum(os.path.join(src, "Symbols", "ResultNameKind.cs"), "ResultNameKind"))
    RESULT_NAME_KINDS.extend(n for n, _ in RESULT_NAME_KINDS_FULL)
    PARAMETER_TYPE_KINDS.extend(n for n, _ in parse_enum(os.path.join(src, "Symbols", "ParameterTypeKind.cs"), "ParameterTypeKind"))

    with open(os.path.join(src, "FunctionHelpers.cs"), encoding="utf-8-sig") as fh:
        m = re.search(r"MaxRepeat\s*=\s*short\.MaxValue", fh.read())
        if not m:
            print("warning: FunctionHelpers.MaxRepeat is no longer short.MaxValue", file=sys.stderr)

    fields = {}
    alls = {}
    for fname, cls in (("Functions.cs", "Functions"), ("Functions.Convert.cs", "Functions"), ("Aggregates.cs", "Aggregates")):
        with open(os.path.join(src, fname), encoding="utf-8-sig") as fh:
            toks = tokenize(fh.read())
        f, al = extract_fields(toks, cls)
        fields.update(f)
        alls.update(al)

    ev = Evaluator(fields)
    entries = []
    errors = []
    for cls, kind in (("Functions", "Scalar"), ("Aggregates", "Aggregate")):
        all_ast = alls.get(cls)
        if all_ast is None or all_ast[0] != "array":
            raise SystemExit(f"could not find {cls}.All")
        for item in all_ast[1]:
            if item[0] != "name":
                errors.append(f"{cls}.All: unsupported element {item!r}")
                continue
            try:
                fn = ev.function(item, cls)
                fn["kind"] = kind
                if fn["result_name"] not in RESULT_NAME_KINDS:
                    raise EvalError(f"unknown result name kind {fn['result_name']}")
                entries.append(fn)
            except (EvalError, KeyError, IndexError) as ex:
                errors.append(f"{cls}.{item[1]}: {ex}")

    # dedupe exact duplicates (same name+kind) keeping the first, report them
    seen = {}
    deduped = []
    for e in entries:
        key = (e["name"], e["kind"])
        if key in seen:
            errors.append(f"duplicate {e['kind']} {e['name']} (kept first)")
            continue
        seen[key] = True
        deduped.append(e)
    deduped.sort(key=lambda e: (e["name"].encode(), 0 if e["kind"] == "Scalar" else 1))

    emit(deduped, ret_kinds, a.out, src)
    ns = sum(1 for e in deduped if e["kind"] == "Scalar")
    na = sum(1 for e in deduped if e["kind"] == "Aggregate")
    print(f"wrote {os.path.normpath(a.out)}: {ns} scalar functions, {na} aggregates")
    for e in errors:
        print("unparsed: " + e, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
