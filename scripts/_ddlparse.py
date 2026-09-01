"""Shared DDL column extraction for profile database docs."""
from __future__ import annotations
import re, pathlib

TYPE_RE = re.compile(
    r'^\s{2,}(?P<name>\w+)\s+(?P<type>STRING|INT64|BOOL|BOOLEAN|FLOAT64|TIMESTAMP|DATE|DATETIME|BYTES|NUMERIC|BIGNUMERIC|ARRAY\s*<.*|STRUCT\s*<.*)',
    re.IGNORECASE)
CREATE_RE = re.compile(r'CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)\.(\w+)\s*\(', re.IGNORECASE)
ALTER_RE = re.compile(
    r'ALTER\s+TABLE\s+(\w+)\.(\w+)\s+ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+(\w+)\s+([A-Z0-9_<>, ]+?)\s*;(?:[^\S\n]*--[^\S\n]*(?P<note>[^\n]*))?',
    re.IGNORECASE)

def _split_block(src: str, start: int) -> str:
    """Return the body text between the CREATE TABLE '(' and its matching ')'."""
    depth, i, out = 0, start, []
    while i < len(src):
        ch = src[i]
        if ch == '(':
            depth += 1
            if depth == 1:
                i += 1
                continue
        elif ch == ')':
            depth -= 1
            if depth == 0:
                break
        out.append(ch)
        i += 1
    return ''.join(out)

def _clean(text: str) -> str:
    text = text.strip().rstrip(',').strip()
    return re.sub(r'\s+', ' ', text)

def parse_columns(ddl_path: pathlib.Path) -> dict[str, list[dict]]:
    """dataset.table -> [{name, type, comment}] from CREATE TABLE + ALTER ADD COLUMN."""
    src = ddl_path.read_text(encoding='utf-8')
    tables: dict[str, list[dict]] = {}
    for m in CREATE_RE.finditer(src):
        key = f"{m.group(1)}.{m.group(2)}"
        body = _split_block(src, m.end() - 1)
        cols: list[dict] = []
        depth_open = 0          # unclosed <> from a STRUCT/ARRAY declaration
        nest_parent = ''        # column that opened the current nested block
        pending: list[str] = []          # section banner comments become context
        for raw in body.splitlines():
            line = raw.rstrip()
            if not line.strip():
                continue
            bare = line.strip()
            if bare.startswith('--'):
                note = bare.lstrip('-').strip(' ─│').strip()
                if note:
                    pending.append(note)
                continue
            tm = TYPE_RE.match(line)
            if not tm:
                # a bare '>' / '>>,' line closes one or more nested STRUCT levels
                closers = bare.count('>') - bare.count('<')
                if closers > 0 and set(bare) <= set('>,; '):
                    depth_open = max(0, depth_open - closers)
                continue
            inline = ''
            if '--' in line:
                inline = _clean(line.split('--', 1)[1])
            coltype = _clean(tm.group('type'))
            # a STRUCT/ARRAY may wrap lines; take the declared text up to the comment
            decl = line.split('--', 1)[0]
            tail = _clean(decl[decl.lower().find(tm.group('name').lower()) + len(tm.group('name')):])
            if tail:
                coltype = tail
            comment = inline or (pending[-1] if pending else '')
            # normalise the declared type: repair a truncated ARRAY<STRING and
            # label multi-line containers rather than emitting a dangling '<'
            norm = coltype
            open_delta = norm.count('<') - norm.count('>')
            if open_delta > 0:
                if norm.rstrip('<').upper().endswith(('ARRAY', 'STRUCT')) or norm.endswith('<'):
                    norm = norm.replace('<', ' of ').strip().rstrip('of').strip() + ' (fields below)'
                else:
                    norm = norm + '>' * open_delta
            entry = {'name': tm.group('name'), 'type': norm, 'comment': comment}
            if depth_open > 0:
                entry['parent'] = nest_parent
                # strip only the delimiters that close the ENCLOSING struct,
                # keeping this field's own ARRAY<...> brackets balanced
                nested = norm.strip().rstrip(',').strip()
                excess = nested.count('>') - nested.count('<')
                if excess > 0:
                    nested = nested[: len(nested) - excess]
                entry['type'] = nested
            else:
                nest_parent = tm.group('name')
            decl_only = line.split('--', 1)[0]
            depth_open += decl_only.count('<') - decl_only.count('>')
            if depth_open < 0:
                depth_open = 0
            cols.append(entry)
            pending.clear()
        if cols:
            tables.setdefault(key, []).extend(cols)
    for m in ALTER_RE.finditer(src):
        key = f"{m.group(1)}.{m.group(2)}"
        if key not in tables:
            tables[key] = []
        if not any(c['name'] == m.group(3) for c in tables[key]):
            tables[key].append({'name': m.group(3), 'type': _clean(m.group(4)),
                                'comment': _clean(m.group('note') or '')})
    return tables

if __name__ == '__main__':
    t = parse_columns(pathlib.Path('sql/profile_database_ddl.sql'))
    for k, v in sorted(t.items()):
        print(f'{k}: {len(v)}')
