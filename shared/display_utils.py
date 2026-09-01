"""
Display utilities for formatted CLI output
"""

def print_header(text, width=70):
    """Print section header."""
    print(f"\n{'=' * width}")
    print(f"{text:^{width}}")
    print(f"{'=' * width}\n")

def print_section(text):
    """Print subsection header."""
    print(f"\n{'-' * 70}")
    print(f"{text}")
    print(f"{'-' * 70}")

def print_key_value(key, value, indent=0):
    """Print key-value pair."""
    spacing = "  " * indent
    print(f"{spacing}{key}: {value}")

def print_table(headers, rows, widths=None):
    """Print formatted table."""
    # Simple table formatting
    if widths:
        col_widths = widths
    else:
        if not rows:
            col_widths = [len(h) for h in headers]
        else:
            col_widths = [max(len(str(row[i])) for row in [headers] + rows) for i in range(len(headers))]

    # Header
    header_row = " | ".join(f"{headers[i]:<{col_widths[i]}}" for i in range(len(headers)))
    print(header_row)
    print("-" * len(header_row))

    # Rows
    for row in rows:
        print(" | ".join(f"{str(row[i]):<{col_widths[i]}}" for i in range(len(row))))

def status_icon(status):
    """Return status icon."""
    icons = {
        'success': '✓',
        'error': '✗',
        'warning': '⚠',
        'info': 'ℹ',
        'running': '⋯'
    }
    return icons.get(status, '?')

def format_number(num):
    """Format number with commas."""
    if isinstance(num, (int, float)):
        return f"{num:,}"
    return str(num)







