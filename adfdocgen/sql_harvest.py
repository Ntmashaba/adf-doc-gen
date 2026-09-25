"""Token-based reader for the tables a SQL script reads, writes and the procedures it runs.

The tokenizer is shared in spirit with pbi-doc-gen: comments, strings (including
Oracle q'' literals) and bracketed, double-quoted or backticked names are handled
before any keyword is looked at, so text inside them can never look like SQL.
"""
import re
from typing import List, Tuple

KEYWORDS = {
    "select", "from", "where", "join", "on", "using", "as", "set", "values", "into",
    "insert", "update", "delete", "merge", "when", "then", "matched", "group", "order",
    "by", "having", "union", "all", "except", "intersect", "inner", "left", "right",
    "full", "outer", "cross", "apply", "with", "table", "top", "distinct", "and", "or",
    "not", "null", "case", "else", "end", "limit", "offset", "fetch", "window",
    "lateral", "natural", "partition", "over", "output", "option", "for", "pivot",
    "unpivot", "qualify", "returning", "exec", "execute", "begin", "declare", "if",
    "while", "return", "go", "statistics", "overwrite", "or", "replace", "temporary",
    "temp", "transient", "volatile", "global", "local", "create", "truncate", "copy",
}
NOISE = {"dual", "unnest", "openjson", "openrowset", "openquery", "string_split",
         "generate_series", "flatten", "table"}
JOINERS = {"join", "apply"}


class Tok:
    __slots__ = ("text", "kind", "low")

    def __init__(self, text, kind="word"):
        self.text, self.kind = text, kind
        self.low = text.lower() if kind == "word" else ""


def tokens(sql: str) -> List[Tok]:
    out, i, n = [], 0, len(sql)
    while i < n:
        c = sql[i]
        if c.isspace():
            i += 1
            continue
        if sql.startswith("--", i):
            end = sql.find("\n", i)
            i = n if end < 0 else end
            continue
        if sql.startswith("/*", i):
            depth, i = 1, i + 2
            while i < n and depth:
                if sql.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                elif sql.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    i += 1
            continue
        if c in "qQ" and i + 2 < n and sql[i + 1] == "'" and not (i and (sql[i - 1].isalnum() or sql[i - 1] == "_")):
            opener = sql[i + 2]
            closer = {"[": "]", "{": "}", "(": ")", "<": ">"}.get(opener, opener)
            end = sql.find(closer + "'", i + 3)
            out.append(Tok("", "string"))
            i = n if end < 0 else end + 2
            continue
        if c in "'\"[`":
            close = "]" if c == "[" else c
            value, i = [], i + 1
            while i < n:
                if sql[i] == close:
                    if i + 1 < n and sql[i + 1] == close:
                        value.append(close)
                        i += 2
                        continue
                    i += 1
                    break
                value.append(sql[i])
                i += 1
            out.append(Tok("".join(value), "string" if c == "'" else "ident"))
            continue
        if c.isalpha() or c in "_#$":
            end = i + 1
            while end < n and (sql[end].isalnum() or sql[end] in "_#$"):
                end += 1
            out.append(Tok(sql[i:end]))
            i = end
            continue
        out.append(Tok(c, "sym"))
        i += 1
    return out


def _quote(part: str) -> str:
    return part if re.fullmatch(r"[\w$#]+", part) else "[" + part.replace("]", "]]") + "]"


class _Scanner:
    def __init__(self, sql: str):
        self.t = tokens(sql or "")
        self.reads, self.writes, self.procs = [], [], []
        self.ctes = set()
        self.aliases = {}
        # For each token, the first token inside its innermost open parenthesis (or None at top level).
        self.ctx, stack = [], []
        for i, tok in enumerate(self.t):
            if tok.kind == "sym" and tok.text == ")" and stack:
                stack.pop()
            self.ctx.append(stack[-1] if stack else None)
            if tok.kind == "sym" and tok.text == "(":
                nxt = self.t[i + 1] if i + 1 < len(self.t) else None
                stack.append(nxt.low if nxt is not None else "")

    def at(self, i):
        return self.t[i] if 0 <= i < len(self.t) else Tok("", "eof")

    def is_sym(self, i, s):
        tok = self.at(i)
        return tok.kind == "sym" and tok.text == s

    def name(self, i) -> Tuple[str, int, bool]:
        """Parse a dotted name at i. Returns (name, next index, is_function_call)."""
        if self.is_sym(i, "@") or self.is_sym(i, "("):
            return "", i, False
        parts = []
        while True:
            tok = self.at(i)
            if tok.kind == "ident" and tok.text:
                parts.append(tok.text)
                i += 1
            elif tok.kind == "word" and (parts or tok.low not in KEYWORDS):
                parts.append(tok.text)
                i += 1
            elif tok.kind == "sym" and tok.text == "." and parts:
                parts.append("")  # db..table
            else:
                break
            if self.is_sym(i, "."):
                i += 1
                continue
            break
        parts = [p for p in parts if p]
        if not parts:
            return "", i, False
        return ".".join(_quote(p) for p in parts), i, self.is_sym(i, "(")

    def keep(self, nm: str, is_call: bool) -> bool:
        if not nm or is_call:
            return False
        leaf = nm.split(".")[-1].strip("[]").lower()
        if nm.startswith("#") or nm.startswith("@") or leaf in NOISE:
            return False
        return not ("." not in nm and nm.strip("[]").lower() in self.ctes)

    def collect_ctes(self):
        for i, tok in enumerate(self.t):
            if tok.low != "with" or self.is_sym(i + 1, "("):
                continue
            j = i + 1
            if self.at(j).low == "recursive":
                j += 1
            while True:
                nm, j, _ = self.name(j)
                if not nm or "." in nm:
                    break
                if self.is_sym(j, "("):  # column list
                    j = self.skip_parens(j)
                if self.at(j).low != "as":
                    break
                self.ctes.add(nm.strip("[]").lower())
                j += 1
                if self.at(j).low in ("materialized", "not"):
                    j += 1 if self.at(j).low == "materialized" else 2
                if not self.is_sym(j, "("):
                    break
                j = self.skip_parens(j)
                if not self.is_sym(j, ","):
                    break
                j += 1

    def skip_parens(self, i):
        depth = 0
        while i < len(self.t):
            if self.is_sym(i, "("):
                depth += 1
            elif self.is_sym(i, ")"):
                depth -= 1
                if depth == 0:
                    return i + 1
            i += 1
        return i

    def query_context(self, i) -> bool:
        ctx = self.ctx[i]
        return ctx is None or ctx in ("select", "with")

    def run(self):
        self.collect_ctes()
        t = self.t
        for i, tok in enumerate(t):
            w = tok.low
            if not w:
                continue
            prev = self.at(i - 1).low
            if w == "from" and self.at(i - 2).low == "delete":
                nm, j, _ = self.name(i + 1)  # DELETE x FROM dbo.T x: remember the alias only
                if self.at(j).low == "as":
                    j += 1
                if nm and self.at(j).kind in ("word", "ident"):
                    self.aliases[self.at(j).text.lower()] = nm
            elif w == "from" and prev != "delete" and self.query_context(i):
                self.read_list(i + 1)
            elif w in JOINERS and self.query_context(i):
                self.read_list(i + 1, single=True)
            elif w == "using" and self.merge_before(i):
                self.add(self.reads, *self.name(i + 1))
            elif w == "insert":
                j = i + 1
                if self.at(j).low in ("into", "overwrite"):
                    j += 1
                if self.at(j).low == "table":
                    j += 1
                self.add(self.writes, *self.name(j))
            elif w == "into" and prev not in ("insert", "merge", "copy") and self.select_before(i):
                self.add(self.writes, *self.name(i + 1))
            elif w == "copy" and self.at(i + 1).low == "into":
                self.add(self.writes, *self.name(i + 2))
            elif w == "update" and prev not in ("then", "on", "for") and self.at(i + 1).low != "statistics":
                self.add(self.writes, *self.name(i + 1))
            elif w == "merge":
                j = i + 2 if self.at(i + 1).low == "into" else i + 1
                self.add(self.writes, *self.name(j))
            elif w == "delete" and prev not in ("then", "on", "for"):
                j = i + 2 if self.at(i + 1).low == "from" else i + 1
                self.add(self.writes, *self.name(j))
            elif w == "truncate" and self.at(i + 1).low == "table":
                self.add(self.writes, *self.name(i + 2))
            elif w == "create":
                j = i + 1
                while self.at(j).low in ("or", "replace", "temporary", "temp", "transient",
                                         "volatile", "global", "local", "multiset", "set"):
                    j += 1
                if self.at(j).low == "table":
                    j += 1
                    if self.at(j).low == "if":  # IF NOT EXISTS
                        j += 3
                    self.add(self.writes, *self.name(j))
            elif w in ("exec", "execute", "call"):
                j = i + 1
                if self.is_sym(j, "@") and self.is_sym(j + 2, "="):
                    j += 3
                nm, _, _ = self.name(j)
                if nm and not nm.startswith("#"):
                    self.procs.append(nm)
        # UPDATE t ... FROM dbo.Target t: the written name is an alias.
        self.writes = [self.aliases.get(w.strip("[]").lower(), w) if "." not in w else w
                       for w in self.writes]
        dedupe = lambda xs: list(dict.fromkeys(xs))
        return dedupe(self.reads), dedupe(self.writes), dedupe(self.procs)

    def add(self, bucket, nm, _i, is_call):
        if self.keep(nm, is_call):
            bucket.append(nm)

    def read_list(self, i, single=False):
        while True:
            nm, i, is_call = self.name(i)
            self.add(self.reads, nm, i, is_call)
            if not nm:
                return
            if self.at(i).low == "as":
                i += 1
            alias = self.at(i)
            if alias.kind in ("word", "ident") and alias.low not in KEYWORDS:
                if not is_call:
                    self.aliases[alias.text.lower()] = nm
                i += 1
            if single or not self.is_sym(i, ","):
                return
            i += 1

    def merge_before(self, i) -> bool:
        for j in range(i - 1, max(-1, i - 40), -1):
            low = self.at(j).low
            if low == "merge":
                return True
            if self.is_sym(j, ";"):
                return False
        return False

    def select_before(self, i) -> bool:
        depth = 0
        for j in range(i - 1, -1, -1):
            tok = self.at(j)
            if tok.kind == "sym" and tok.text == ")":
                depth += 1
            elif tok.kind == "sym" and tok.text == "(":
                if depth == 0:
                    return False
                depth -= 1
            elif depth == 0 and (tok.low in ("select", "fetch") or (tok.kind == "sym" and tok.text == ";")):
                return tok.low == "select"
        return False


def harvest_sql(sql_text: str) -> Tuple[List[str], List[str], List[str]]:
    """Return (tables_read, tables_written, procs_executed) from a SQL string."""
    if not sql_text:
        return [], [], []
    return _Scanner(sql_text).run()
