"""Parse the identity hub DDL, config and connectors for doc generation.

Kept separate from the generator so the markdown template does not have to be
escaped inside this module.
"""

from __future__ import annotations

import ast
import re

# `name TYPE [NOT NULL] [OPTIONS(description='...')]` inside a CREATE TABLE body
COL_RE = re.compile(
    r"^\s{2,}(?P<name>\w+)\s+"
    r"(?P<type>STRING|INT64|BOOL|BOOLEAN|FLOAT64|TIMESTAMP|DATE|DATETIME|BYTES|NUMERIC|"
    r"ARRAY\s*<[^>]*>|STRUCT\s*<.*)"
    r"(?P<notnull>\s+NOT\s+NULL)?"
    r"(?:\s+OPTIONS\(\s*description\s*=\s*(?P<q>['\"])(?P<desc>.*?)(?P=q)\s*\))?",
    re.IGNORECASE,
)
CREATE_RE = re.compile(
    r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+`?[\w-]*\.?(?P<dataset>\w+)\.(?P<table>\w+)`?\s*\(",
    re.IGNORECASE,
)
KEY_RE = re.compile(r"^\s*(PRIMARY KEY|FOREIGN KEY)", re.IGNORECASE)


def _block(src: str, open_paren: int) -> str:
    depth, i, out = 0, open_paren, []
    while i < len(src):
        ch = src[i]
        if ch == "(":
            depth += 1
            if depth == 1:
                i += 1
                continue
        elif ch == ")":
            depth -= 1
            if depth == 0:
                break
        out.append(ch)
        i += 1
    return "".join(out)


def parse_ddl(path):
    """dataset.table -> {'comment': str, 'columns': [{name,type,required,desc}]}"""
    src = path.read_text(encoding="utf-8")
    lines = src.splitlines()
    tables = {}
    for m in CREATE_RE.finditer(src):
        key = f"{m.group('dataset')}.{m.group('table')}"
        # the banner comment block immediately above the CREATE
        line_no = src[: m.start()].count("\n")
        # Walk up through the comment banner, stopping at the ruler line that
        # separates one table's banner from the previous section. Without the
        # stop, the first table absorbs the whole file header.
        banner = []
        j = line_no - 1
        while j >= 0:
            stripped = lines[j].strip()
            if not stripped:
                j -= 1
                continue
            if not stripped.startswith("--"):
                break
            text = stripped.lstrip("-").strip()
            if not text or set(text) <= {"-", "="}:
                if banner:            # ruler above the banner: stop here
                    break
                j -= 1
                continue
            banner.insert(0, text)
            j -= 1
        # Drop the redundant "table_name  --  " prefix the banners start with
        if banner:
            head = banner[0]
            if "--" in head:
                lead, _, rest = head.partition("--")
                if lead.strip() == m.group("table"):
                    banner[0] = rest.strip()
        body = _block(src, m.end() - 1)
        cols = []
        for raw in body.splitlines():
            if not raw.strip() or KEY_RE.match(raw):
                continue
            cm = COL_RE.match(raw)
            if not cm:
                continue
            cols.append(
                {
                    "name": cm.group("name"),
                    "type": re.sub(r"\s+", " ", cm.group("type").strip()),
                    "required": bool(cm.group("notnull")),
                    "desc": (cm.group("desc") or "").strip(),
                }
            )
        if cols:
            tables[key] = {"comment": " ".join(banner), "columns": cols}
    return tables


def parse_connectors(hub_py):
    """connector method name -> {match_rules, source_tables, doc}"""
    src = hub_py.read_text(encoding="utf-8")
    tree = ast.parse(src)
    out = {}
    for node in ast.walk(tree):
        if not (isinstance(node, ast.FunctionDef) and node.name.startswith("connect_")):
            continue
        seg = ast.get_source_segment(src, node) or ""
        rules = sorted(set(re.findall(r"match_rule=[\"']([A-Z_0-9]+)[\"']", seg)))
        tables = sorted(
            set(re.findall(r"FROM\s+`([A-Za-z0-9_.{}]+)`", seg))
            | set(re.findall(r"JOIN\s+`([A-Za-z0-9_.{}]+)`", seg))
        )
        doc = ast.get_docstring(node) or ""
        out[node.name] = {
            "match_rules": rules,
            "source_tables": [t for t in tables if not t.startswith("{")],
            "doc": doc.strip().split("\n\n")[0].replace("\n", " ").strip(),
        }
    return out


def parse_run_order(hub_py):
    """[(phase_label, connector_method)] in the order the pipeline runs them."""
    src = hub_py.read_text(encoding="utf-8")
    order, phase = [], ""
    for line in src.splitlines():
        pm = re.search(r'_phase\(\s*[fr]?["\'](.+?)["\']\s*\)', line)
        if pm:
            phase = pm.group(1)
            continue
        cm = re.search(
            r'_run_connector\(\s*["\'][\w]+["\']\s*,\s*self\.(\w+)\s*\)', line
        )
        if cm:
            order.append((phase, cm.group(1)))
    return order


def connector_config(cfg):
    """connector name -> its config dict, from identity_hub.yaml"""
    hub = cfg.get("identity_hub", cfg)
    return hub.get("connectors", {}) or {}
