#!/usr/bin/env python3
"""
One-time migration script for YAML config standardization.

Changes:
1. INTEGER → INT64, FLOAT → FLOAT64 in schema type definitions
2. table_groups → groups (mailchimp)
3. Restructure incremental config keys into nested `incremental:` block
4. Add source.name to configs missing it (dcm)

Run: python migrate_yaml_configs.py
"""

import re
import sys
from pathlib import Path


def migrate_schema_types(text: str) -> str:
    """Replace legacy BigQuery types with canonical ones."""
    # Only replace when it appears as a YAML value (after colon+space)
    text = re.sub(r': INTEGER\b', ': INT64', text)
    text = re.sub(r': FLOAT\b', ': FLOAT64', text)
    return text


def migrate_table_groups(text: str) -> str:
    """Rename top-level table_groups key to groups."""
    # Only match at the start of a line (top-level key)
    text = re.sub(r'^table_groups:', 'groups:', text, flags=re.MULTILINE)
    return text


def migrate_incremental_config(text: str) -> str:
    """
    Restructure scattered incremental keys into a nested incremental: block.

    Handles these patterns at resource level (indented under a resource name):
    - incremental_strategy: <value>
    - incremental_date_fields: [...]
    - incremental_field: <value>
    - incremental_lookback_days: <value>
    - initial_value: <value>

    Converts to:
    incremental:
      strategy: <value>
      fields: [...]
      lookback_days: <value>
      initial_value: <value>
    """
    lines = text.split('\n')
    result = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # Detect if we're inside a resource block by checking indent level
        # Resource-level keys are typically at 4 spaces indent
        match_strategy = re.match(r'^(\s+)incremental_strategy:\s*(.+)$', line)
        match_field = re.match(r'^(\s+)incremental_field:\s*(.+)$', line)
        match_date_fields = re.match(r'^(\s+)incremental_date_fields:\s*$', line)
        match_date_fields_inline = re.match(r'^(\s+)incremental_date_fields:\s*\[(.+)\]$', line)
        match_lookback = re.match(r'^(\s+)incremental_lookback_days:\s*(.+)$', line)

        # Check if this line starts an incremental block we need to migrate
        if match_strategy or match_field or match_date_fields or match_date_fields_inline:
            # Collect all incremental-related keys at this indent level
            if match_strategy:
                indent = match_strategy.group(1)
            elif match_field:
                indent = match_field.group(1)
            elif match_date_fields:
                indent = match_date_fields.group(1)
            elif match_date_fields_inline:
                indent = match_date_fields_inline.group(1)

            indent_len = len(indent)

            # Collect all incremental keys and initial_value at this indent
            inc_keys = {}
            date_fields_list = []
            consumed_lines = []

            # Look at current line and subsequent lines at same indent
            j = i
            while j < len(lines):
                curr = lines[j]

                # Check if this line is at the expected indent level and is an incremental key
                m_strategy = re.match(rf'^{re.escape(indent)}incremental_strategy:\s*(.+)$', curr)
                m_field = re.match(rf'^{re.escape(indent)}incremental_field:\s*(.+)$', curr)
                m_date_fields = re.match(rf'^{re.escape(indent)}incremental_date_fields:\s*$', curr)
                m_date_fields_inline = re.match(rf'^{re.escape(indent)}incremental_date_fields:\s*\[(.+)\]$', curr)
                m_lookback = re.match(rf'^{re.escape(indent)}incremental_lookback_days:\s*(.+)$', curr)
                m_initial = re.match(rf'^{re.escape(indent)}initial_value:\s*(.+)$', curr)

                if m_strategy:
                    inc_keys['strategy'] = m_strategy.group(1).strip()
                    consumed_lines.append(j)
                    j += 1
                elif m_field:
                    val = m_field.group(1).strip()
                    if val != 'null' and val != '~' and val != 'None':
                        inc_keys['field'] = val
                    consumed_lines.append(j)
                    j += 1
                elif m_date_fields:
                    consumed_lines.append(j)
                    j += 1
                    # Consume the list items
                    while j < len(lines):
                        list_item = re.match(rf'^{re.escape(indent)}- (.+)$', lines[j])
                        if list_item:
                            date_fields_list.append(list_item.group(1).strip())
                            consumed_lines.append(j)
                            j += 1
                        else:
                            break
                elif m_date_fields_inline:
                    items = m_date_fields_inline.group(1)
                    date_fields_list = [x.strip().strip("'\"") for x in items.split(',')]
                    consumed_lines.append(j)
                    j += 1
                elif m_lookback:
                    inc_keys['lookback_days'] = m_lookback.group(1).strip()
                    consumed_lines.append(j)
                    j += 1
                elif m_initial:
                    inc_keys['initial_value'] = m_initial.group(1).strip()
                    consumed_lines.append(j)
                    j += 1
                elif j == i:
                    # First line must be consumed
                    j += 1
                else:
                    # Not an incremental key - stop looking ahead
                    # But only if we're past the initial line group
                    break

            # Build the new incremental block
            child_indent = indent + '  '

            # Determine strategy
            strategy = inc_keys.get('strategy')
            if not strategy:
                if inc_keys.get('field'):
                    strategy = 'date'
                else:
                    strategy = 'none'

            # Determine fields
            fields = date_fields_list
            if not fields and inc_keys.get('field'):
                fields = [inc_keys['field']]

            # Build output
            new_lines = [f'{indent}incremental:']
            new_lines.append(f'{child_indent}strategy: {strategy}')

            if fields and strategy not in ('none', 'id', 'via_parent'):
                if len(fields) == 1:
                    new_lines.append(f'{child_indent}fields:')
                    new_lines.append(f'{child_indent}- {fields[0]}')
                else:
                    new_lines.append(f'{child_indent}fields:')
                    for f in fields:
                        new_lines.append(f'{child_indent}- {f}')

            if 'lookback_days' in inc_keys:
                new_lines.append(f'{child_indent}lookback_days: {inc_keys["lookback_days"]}')

            if 'initial_value' in inc_keys:
                new_lines.append(f'{child_indent}initial_value: {inc_keys["initial_value"]}')

            # Skip consumed lines by marking them
            # First, add everything before the first consumed line
            # Then add the new block, then continue after last consumed line
            result.extend(new_lines)
            i = max(consumed_lines) + 1 if consumed_lines else j

        elif match_lookback and not any(
            re.match(r'^\s+incremental_strategy:', lines[k]) or
            re.match(r'^\s+incremental_field:', lines[k]) or
            re.match(r'^\s+incremental_date_fields:', lines[k])
            for k in range(max(0, i - 5), i)
        ):
            # Standalone lookback without strategy/field (shouldn't normally happen)
            # Just leave it as is
            result.append(line)
            i += 1
        else:
            result.append(line)
            i += 1

    return '\n'.join(result)


def add_source_name(text: str, name: str) -> str:
    """Add source.name if missing."""
    if re.search(r'^\s+name:', text, re.MULTILINE):
        return text
    text = re.sub(
        r'^(source:\s*\n)',
        f'\\1  name: {name}\n',
        text,
        flags=re.MULTILINE,
    )
    return text


def migrate_file(filepath: Path) -> bool:
    """Apply all migrations to a single YAML file. Returns True if changed."""
    original = filepath.read_text(encoding='utf-8')
    text = original

    # 1. Schema type renames
    text = migrate_schema_types(text)

    # 2. table_groups rename
    text = migrate_table_groups(text)

    # 3. Incremental config restructure
    text = migrate_incremental_config(text)

    # 4. Add source.name for DCM
    if filepath.name == 'dcm.yaml':
        text = add_source_name(text, 'dcm')

    if text != original:
        filepath.write_text(text, encoding='utf-8')
        return True
    return False


def main():
    configs_dir = Path(__file__).parent / 'configs'

    target_files = [
        'wordpress.yaml',
        'mailchimp.yaml',
        'dcm.yaml',
        'facebook.yaml',
        'npi_registry.yaml',
        'instagram.yaml',
        'threads.yaml',
        'onetrust.yaml',
        'screaming_frog.yaml',
        'limesurvey.yaml',
    ]

    changed = []
    for name in target_files:
        path = configs_dir / name
        if not path.exists():
            print(f"SKIP  {name} (not found)")
            continue
        if migrate_file(path):
            changed.append(name)
            print(f"OK    {name}")
        else:
            print(f"SAME  {name} (no changes)")

    print(f"\nMigrated {len(changed)} files: {', '.join(changed)}")


if __name__ == '__main__':
    main()
