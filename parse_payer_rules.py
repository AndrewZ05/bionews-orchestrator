#!/usr/bin/env python3
"""
Parse npi_create_doctor_payers.sql and extract all patterns into structured data
for the table-driven refactoring.

Outputs 4 sections:
1. ref_payer_exclusions INSERT statements
2. ref_payer_category_rules INSERT statements
3. ref_payer_rules INSERT statements
4. ref_payer_parents INSERT statements
"""

import re
import sys
from pathlib import Path


def parse_exclusions(lines):
    """Parse the WHERE NOT (...) block into exclusion rules."""
    rules = []
    eid = 1
    in_block = False
    category = 'MISC'

    for line in lines:
        stripped = line.strip()

        # Detect start of WHERE NOT block
        if 'WHERE NOT (' in stripped:
            in_block = True
            continue

        # Detect end
        if in_block and stripped.startswith(')') and not stripped.startswith('OR'):
            break

        if not in_block:
            continue

        # Category comments
        if stripped.startswith('--'):
            comment = stripped.lstrip('- ').strip()
            if comment:
                category = comment.split('(')[0].strip()[:30]
            continue

        # Skip blank
        if not stripped or stripped == 'OR':
            continue

        # Remove leading OR
        pat_line = stripped
        if pat_line.startswith('OR '):
            pat_line = pat_line[3:]

        # Parse each OR-condition
        conditions = split_or_conditions(pat_line)
        for cond in conditions:
            parsed = parse_single_condition(cond)
            if parsed:
                match_type, pattern = parsed
                rules.append((eid, match_type, pattern, category, ''))
                eid += 1

    return rules


def parse_category_rules(lines):
    """Parse the payer_category CASE block."""
    rules = []
    rid = 1
    priority = 10
    in_block = False
    current_category = None

    for i, line in enumerate(lines):
        stripped = line.strip()

        if 'PAYER CATEGORY' in stripped:
            in_block = True
            continue

        if in_block and 'PAYER NAME' in stripped:
            break

        if not in_block:
            continue

        if stripped.startswith('CASE'):
            continue

        if stripped.startswith('END AS payer_category'):
            break

        # Detect THEN clause
        if "THEN 'MEDICAID'" in stripped or "THEN 'MEDICARE'" in stripped or \
           "THEN 'MILITARY'" in stripped or "THEN 'COMMERCIAL'" in stripped:
            m = re.search(r"THEN '(\w+)'", stripped)
            if m:
                current_category = m.group(1)
            # Also check if this line has a WHEN/OR condition before THEN
            before_then = stripped.split('THEN')[0].strip()
            if before_then and ('issuer_raw' in before_then or 'type_code' in before_then):
                conditions = split_or_conditions(before_then)
                for cond in conditions:
                    parsed = parse_category_condition(cond)
                    if parsed:
                        match_field, match_type, pattern = parsed
                        rules.append((rid, priority, match_type, pattern, match_field, current_category))
                        rid += 1
                        priority += 10
            continue

        # WHEN or OR lines before THEN
        if ('issuer_raw' in stripped or 'type_code' in stripped) and current_category is None:
            # We don't know category yet, will be set by THEN
            pass

        if stripped.startswith('WHEN') or stripped.startswith('OR'):
            pat_line = stripped
            if pat_line.startswith('WHEN '):
                pat_line = pat_line[5:]
            elif pat_line.startswith('OR '):
                pat_line = pat_line[3:]

            conditions = split_or_conditions(pat_line)
            for cond in conditions:
                parsed = parse_category_condition(cond)
                if parsed and current_category:
                    match_field, match_type, pattern = parsed
                    rules.append((rid, priority, match_type, pattern, match_field, current_category))
                    rid += 1
                    priority += 10

    return rules


def parse_payer_name_rules(lines):
    """Parse the payer_name CASE block into rules with scope and priority.

    Key insight: patterns appear BEFORE their THEN clause, so we buffer
    patterns and assign them when THEN is encountered.
    """
    rules = []
    rid = 1
    priority = 100
    in_payer_case = False
    in_bcbs_outer = False
    in_bcbs_sub = False
    bcbs_sub_priority = 10

    # Buffer for patterns collected before THEN is seen
    pending_patterns = []  # list of (match_type, pattern, neg_pattern, neg_match_type)
    pending_negative = None  # (neg_match_type, neg_pattern) for current WHEN block

    for i, line in enumerate(lines):
        stripped = line.strip()

        if 'PAYER NAME' in stripped and 'Tiered' in stripped:
            in_payer_case = True
            continue

        if not in_payer_case:
            continue

        # End of payer_name CASE
        if stripped == 'END AS payer_name,':
            break

        # Skip comments and blank lines
        if stripped.startswith('--') or not stripped:
            continue

        # Detect BCBS outer gate start
        if stripped.startswith('WHEN issuer_raw LIKE') and 'BCBS' in stripped and not in_bcbs_outer:
            for j in range(i+1, min(i+100, len(lines))):
                if 'THEN CASE' in lines[j]:
                    in_bcbs_outer = True
                    break
                if lines[j].strip().startswith("THEN '"):
                    break

        if in_bcbs_outer and not in_bcbs_sub:
            # Collecting BCBS gate patterns
            if 'THEN CASE' in stripped:
                in_bcbs_sub = True
                bcbs_sub_priority = 10
                pending_patterns = []
                pending_negative = None
                continue

            if stripped.startswith('WHEN') or stripped.startswith('OR'):
                pat_line = stripped
                if pat_line.startswith('WHEN '):
                    pat_line = pat_line[5:]
                elif pat_line.startswith('OR '):
                    pat_line = pat_line[3:]
                pat_line = re.sub(r'\s+THEN\s+CASE\s*$', '', pat_line)

                conditions = split_or_conditions(pat_line)
                for cond in conditions:
                    parsed = parse_single_condition(cond)
                    if parsed:
                        match_type, pattern = parsed
                        rules.append((rid, 100, match_type, pattern, 'BlueCross BlueShield', 'BCBS_GATE', None, None, ''))
                        rid += 1
            continue

        if in_bcbs_sub:
            # Inside BCBS nested CASE
            if stripped.startswith('ELSE') and "'BlueCross BlueShield'" in stripped:
                in_bcbs_sub = False
                in_bcbs_outer = False
                pending_patterns = []
                pending_negative = None
                continue
            if stripped == 'END':
                in_bcbs_sub = False
                in_bcbs_outer = False
                pending_patterns = []
                pending_negative = None
                continue

            # Check for THEN on this line
            then_match = re.search(r"THEN\s+'([^']+)'", stripped)
            if then_match:
                payer_name = then_match.group(1)
                # Also parse conditions on the same line before THEN
                before_then = stripped.split('THEN')[0].strip()
                if before_then.startswith('WHEN '):
                    before_then = before_then[5:]
                elif before_then.startswith('OR '):
                    before_then = before_then[3:]
                if before_then and 'issuer_raw' in before_then:
                    conditions = split_or_conditions(before_then)
                    for cond in conditions:
                        parsed = parse_single_condition(cond)
                        if parsed:
                            pending_patterns.append((parsed[0], parsed[1], None, None))

                # Flush all pending patterns with this payer_name
                for mt, pat, neg_p, neg_mt in pending_patterns:
                    rules.append((rid, bcbs_sub_priority, mt, pat, payer_name, 'BCBS_SUB', neg_p, neg_mt, ''))
                    rid += 1
                pending_patterns = []
                pending_negative = None
                bcbs_sub_priority += 10
                continue

            # Buffer WHEN/OR conditions
            if stripped.startswith('WHEN') or stripped.startswith('OR'):
                # New WHEN = new group — flush should have happened at THEN
                if stripped.startswith('WHEN '):
                    pending_patterns = []
                    pending_negative = None

                pat_line = stripped
                if pat_line.startswith('WHEN '):
                    pat_line = pat_line[5:]
                elif pat_line.startswith('OR '):
                    pat_line = pat_line[3:]

                conditions = split_or_conditions(pat_line)
                for cond in conditions:
                    parsed = parse_single_condition(cond)
                    if parsed:
                        pending_patterns.append((parsed[0], parsed[1], None, None))
            continue

        # =====================
        # GLOBAL rules (non-BCBS)
        # =====================

        # ELSE issuer_raw (Tier 3 fallback) - stop
        if stripped.startswith('ELSE issuer_raw'):
            break

        # Check for THEN on this line
        then_match = re.search(r"THEN\s+'([^']+)'", stripped)
        if then_match:
            payer_name = then_match.group(1)
            # Parse conditions on the same line before THEN
            before_then = stripped.split('THEN')[0].strip()
            if before_then.startswith('WHEN '):
                before_then = before_then[5:]
            elif before_then.startswith('OR '):
                before_then = before_then[3:]

            if before_then and 'issuer_raw' in before_then:
                neg_match_type, neg_pattern, clean_cond = extract_negative(before_then)
                if neg_match_type:
                    pending_negative = (neg_match_type, neg_pattern)
                conditions = split_or_conditions(clean_cond)
                for cond in conditions:
                    parsed = parse_single_condition(cond)
                    if parsed:
                        neg_p = pending_negative[1] if pending_negative else None
                        neg_mt = pending_negative[0] if pending_negative else None
                        pending_patterns.append((parsed[0], parsed[1], neg_p, neg_mt))

            # Also check for THEN 'X' where X = issuer_raw (passthrough)
            if payer_name == 'issuer_raw':
                pending_patterns = []
                pending_negative = None
                continue

            # Flush all pending patterns
            for mt, pat, neg_p, neg_mt in pending_patterns:
                rules.append((rid, priority, mt, pat, payer_name, 'GLOBAL', neg_p, neg_mt, ''))
                rid += 1
            pending_patterns = []
            pending_negative = None
            priority += 10
            continue

        # Buffer WHEN/OR conditions (before THEN is seen)
        if stripped.startswith('WHEN') or stripped.startswith('OR'):
            if stripped.startswith('WHEN '):
                # New WHEN block — clear buffer
                pending_patterns = []
                pending_negative = None

            pat_line = stripped
            if pat_line.startswith('WHEN '):
                pat_line = pat_line[5:]
            elif pat_line.startswith('OR '):
                pat_line = pat_line[3:]

            neg_match_type, neg_pattern, clean_line = extract_negative(pat_line)
            if neg_match_type:
                pending_negative = (neg_match_type, neg_pattern)

            conditions = split_or_conditions(clean_line)
            for cond in conditions:
                parsed = parse_single_condition(cond)
                if parsed:
                    neg_p = pending_negative[1] if pending_negative else None
                    neg_mt = pending_negative[0] if pending_negative else None
                    pending_patterns.append((parsed[0], parsed[1], neg_p, neg_mt))

    return rules


def extract_negative(condition_str):
    """Extract AND NOT LIKE conditions from a pattern line."""
    neg_match_type = None
    neg_pattern = None
    clean = condition_str

    # Pattern: AND issuer_raw NOT LIKE '%FOO%'
    m = re.search(r"AND\s+issuer_raw\s+NOT\s+LIKE\s+'([^']+)'", condition_str)
    if m:
        raw_pat = m.group(1)
        clean = condition_str[:m.start()] + condition_str[m.end():]
        if raw_pat.startswith('%') and raw_pat.endswith('%'):
            neg_match_type = 'CONTAINS'
            neg_pattern = raw_pat.strip('%')
        elif raw_pat.endswith('%'):
            neg_match_type = 'PREFIX'
            neg_pattern = raw_pat.rstrip('%')
        elif raw_pat.startswith('%'):
            neg_match_type = 'SUFFIX'
            neg_pattern = raw_pat.lstrip('%')
        else:
            neg_match_type = 'EXACT'
            neg_pattern = raw_pat

    return neg_match_type, neg_pattern, clean.strip()


def parse_parent_mappings(lines):
    """Parse the with_parent CASE block."""
    rules = []
    in_block = False
    priority = 100

    for line in lines:
        stripped = line.strip()

        if 'with_parent AS' in stripped:
            in_block = True
            continue

        if not in_block:
            continue

        if stripped == 'END AS payer_parent':
            break

        # WHEN payer_name = 'X' THEN 'Y'
        m = re.search(r"WHEN payer_name\s*=\s*'([^']+)'\s+THEN\s+'([^']+)'", stripped)
        if m:
            rules.append((m.group(1), 'EXACT', m.group(2), priority))
            continue

        # WHEN payer_name IN ('X', 'Y') THEN 'Z'
        m = re.search(r"WHEN payer_name\s+IN\s*\(([^)]+)\)\s*THEN\s+'([^']+)'", stripped)
        if m:
            names = re.findall(r"'([^']+)'", m.group(1))
            parent = m.group(2)
            for name in names:
                rules.append((name, 'EXACT', parent, priority))
            continue

        # WHEN payer_name LIKE 'X%' THEN 'Y'
        m = re.search(r"WHEN payer_name\s+LIKE\s+'([^']+)'\s+THEN\s+'([^']+)'", stripped)
        if m:
            pat = m.group(1)
            parent = m.group(2)
            if pat.endswith('%') and not pat.startswith('%'):
                rules.append((pat.rstrip('%'), 'PREFIX', parent, priority))
            elif pat.startswith('%') and pat.endswith('%'):
                rules.append((pat.strip('%'), 'CONTAINS', parent, priority + 50))
            else:
                rules.append((pat, 'EXACT', parent, priority))
            continue

        # Multi-line: THEN on next line
        m = re.search(r"THEN\s+'([^']+)'", stripped)
        if m and 'WHEN' not in stripped:
            # This is a continuation THEN - associate with previous WHEN
            pass

    return rules


def split_or_conditions(line):
    """Split a line with OR conditions into individual conditions."""
    # Remove trailing THEN clause
    line = re.sub(r"\s+THEN\s+(CASE\s*)?'[^']*'.*$", '', line)
    line = re.sub(r"\s+THEN\s+FALSE.*$", '', line)
    line = re.sub(r"\s+THEN\s+TRUE.*$", '', line)
    line = re.sub(r"\s+THEN\s+CASE\s*$", '', line)

    # Split on OR (but not inside strings)
    parts = []
    current = ''
    in_str = False
    tokens = line.split(' OR ')

    # Simple split - works for most cases
    for token in tokens:
        t = token.strip()
        if t:
            parts.append(t)

    return parts


def parse_single_condition(cond):
    """Parse a single condition into (match_type, pattern)."""
    cond = cond.strip()

    # Remove leading WHEN/OR
    if cond.startswith('WHEN '):
        cond = cond[5:]
    if cond.startswith('OR '):
        cond = cond[3:]

    # issuer_raw LIKE '%FOO%'
    m = re.match(r"issuer_raw\s+LIKE\s+'([^']+)'", cond)
    if m:
        pat = m.group(1)
        if pat.startswith('%') and pat.endswith('%'):
            return ('CONTAINS', pat[1:-1])
        elif pat.endswith('%') and not pat.startswith('%'):
            return ('PREFIX', pat[:-1])
        elif pat.startswith('%') and not pat.endswith('%'):
            return ('SUFFIX', pat[1:])
        else:
            return ('EXACT', pat)

    # issuer_raw = 'FOO'
    m = re.match(r"issuer_raw\s*=\s*'([^']*)'", cond)
    if m:
        return ('EXACT', m.group(1))

    # issuer_raw IN ('A', 'B', 'C')
    m = re.match(r"issuer_raw\s+IN\s*\(([^)]+)\)", cond)
    if m:
        # Return first value - caller should handle IN lists
        vals = re.findall(r"'([^']+)'", m.group(1))
        if vals:
            return ('EXACT', vals[0])  # Will need special handling

    # REGEXP_CONTAINS(issuer_raw, r'...')
    m = re.match(r"REGEXP_CONTAINS\(issuer_raw,\s*r'([^']+)'\)", cond)
    if m:
        return ('REGEX', m.group(1))

    # type_code = '05'
    m = re.match(r"type_code\s*=\s*'([^']*)'", cond)
    if m:
        return ('EXACT', m.group(1))

    # type_code IN (...)
    m = re.match(r"type_code\s+IN\s*\(([^)]+)\)", cond)
    if m:
        vals = re.findall(r"'([^']+)'", m.group(1))
        if vals:
            return ('EXACT', vals[0])

    return None


def parse_category_condition(cond):
    """Parse a category condition into (match_field, match_type, pattern)."""
    cond = cond.strip()
    if cond.startswith('WHEN '):
        cond = cond[5:]
    if cond.startswith('OR '):
        cond = cond[3:]

    # type_code = '05'
    m = re.match(r"type_code\s*=\s*'([^']*)'", cond)
    if m:
        return ('TYPE_CODE', 'EXACT', m.group(1))

    # issuer_raw patterns
    parsed = parse_single_condition(cond)
    if parsed:
        match_type, pattern = parsed
        return ('ISSUER_RAW', match_type, pattern)

    return None


def format_sql_string(s):
    """Escape a string for SQL insertion."""
    if s is None:
        return 'NULL'
    return "'" + s.replace("'", "''") + "'"


def main():
    sql_path = Path('sql/npi_create_doctor_payers.sql')
    if not sql_path.exists():
        print(f"File not found: {sql_path}", file=sys.stderr)
        sys.exit(1)

    lines = sql_path.read_text(encoding='utf-8').split('\n')

    print("=== PARSING EXCLUSIONS ===")
    exclusions = parse_exclusions(lines)
    print(f"Found {len(exclusions)} exclusion rules")

    print("\n=== PARSING CATEGORY RULES ===")
    categories = parse_category_rules(lines)
    print(f"Found {len(categories)} category rules")

    print("\n=== PARSING PAYER NAME RULES ===")
    payer_rules = parse_payer_name_rules(lines)
    bcbs_gate = [r for r in payer_rules if r[5] == 'BCBS_GATE']
    bcbs_sub = [r for r in payer_rules if r[5] == 'BCBS_SUB']
    global_rules = [r for r in payer_rules if r[5] == 'GLOBAL']
    print(f"Found {len(payer_rules)} payer rules: {len(bcbs_gate)} BCBS_GATE, {len(bcbs_sub)} BCBS_SUB, {len(global_rules)} GLOBAL")

    print("\n=== PARSING PARENT MAPPINGS ===")
    parents = parse_parent_mappings(lines)
    print(f"Found {len(parents)} parent mappings")

    # Output SQL INSERT statements
    output_path = Path('sql/npi_seed_payer_reference_data.sql')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("-- Auto-generated from parse_payer_rules.py\n")
        f.write("-- Review and adjust before running\n\n")

        # Exclusions
        f.write("-- =============================================\n")
        f.write("-- ref_payer_exclusions\n")
        f.write("-- =============================================\n")
        f.write("INSERT INTO npi_data.ref_payer_exclusions (exclusion_id, match_type, pattern, category, notes) VALUES\n")
        for i, (eid, mt, pat, cat, notes) in enumerate(exclusions):
            comma = ',' if i < len(exclusions) - 1 else ';'
            f.write(f"  ({eid}, {format_sql_string(mt)}, {format_sql_string(pat)}, {format_sql_string(cat)}, {format_sql_string(notes)}){comma}\n")

        f.write("\n")

        # Category rules
        f.write("-- =============================================\n")
        f.write("-- ref_payer_category_rules\n")
        f.write("-- =============================================\n")
        f.write("INSERT INTO npi_data.ref_payer_category_rules (rule_id, priority, match_type, pattern, match_field, payer_category) VALUES\n")
        for i, (rid, pri, mt, pat, mf, cat) in enumerate(categories):
            comma = ',' if i < len(categories) - 1 else ';'
            f.write(f"  ({rid}, {pri}, {format_sql_string(mt)}, {format_sql_string(pat)}, {format_sql_string(mf)}, {format_sql_string(cat)}){comma}\n")

        f.write("\n")

        # Payer rules
        f.write("-- =============================================\n")
        f.write("-- ref_payer_rules\n")
        f.write("-- =============================================\n")
        f.write("INSERT INTO npi_data.ref_payer_rules (rule_id, priority, match_type, pattern, payer_name, rule_scope, negative_pattern, negative_match_type, notes) VALUES\n")
        for i, rule in enumerate(payer_rules):
            rid, pri, mt, pat, pn, scope, neg_p, neg_mt, notes = rule
            comma = ',' if i < len(payer_rules) - 1 else ';'
            f.write(f"  ({rid}, {pri}, {format_sql_string(mt)}, {format_sql_string(pat)}, {format_sql_string(pn)}, {format_sql_string(scope)}, {format_sql_string(neg_p) if neg_p else 'NULL'}, {format_sql_string(neg_mt) if neg_mt else 'NULL'}, {format_sql_string(notes)}){comma}\n")

        f.write("\n")

        # Parent mappings
        f.write("-- =============================================\n")
        f.write("-- ref_payer_parents\n")
        f.write("-- =============================================\n")
        f.write("INSERT INTO npi_data.ref_payer_parents (payer_name, match_type, parent_company, priority) VALUES\n")
        for i, (pn, mt, pc, pri) in enumerate(parents):
            comma = ',' if i < len(parents) - 1 else ';'
            f.write(f"  ({format_sql_string(pn)}, {format_sql_string(mt)}, {format_sql_string(pc)}, {pri}){comma}\n")

    print(f"\nOutput written to {output_path}")


if __name__ == '__main__':
    main()
