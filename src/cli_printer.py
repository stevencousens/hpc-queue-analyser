from rich.console import Console
from rich.table import Table
from rich.columns import Columns
from src.styles import CMAP_RUNNING, CMAP_PENDING
from src.widgets import get_row_color

console = Console()

def print_analysis_group_block(running_group, pending_group):
    """Print summary and allocation tables for a pair of AnalysisGroup objects side by side."""
    console.rule(f"[bold blue]{running_group.name.upper()}")

    # Summary tables
    running_summary = make_summary_table(running_group.summary_stats_df, "Running")
    pending_summary = make_summary_table(pending_group.summary_stats_df, "Pending")
    console.print(Columns([running_summary, pending_summary], equal=True, expand=True))

    # Allocation tables
    running_alloc = make_allocation_table(running_group.allocation_df, "Running", CMAP_RUNNING)
    pending_alloc = make_allocation_table(pending_group.allocation_df, "Pending", CMAP_PENDING)
    console.print(Columns([running_alloc, pending_alloc], equal=True, expand=True))


def make_summary_table(df, label):
    """Convert a summary DataFrame into a Rich table."""
    table = Table(title=f"{label} Summary", show_header=True, header_style="bold cyan")
    table.add_column("Metric", justify="left", width=24)
    table.add_column("Value", justify="right", width=20)

    for row in df.itertuples(index=False):
        table.add_row(str(row.Metric), str(row.Value))

    return table


def make_allocation_table(df, title, cmap):
    """Convert an allocation DataFrame into a Rich table with row colouring."""
    table = Table(title=f"{title} Allocation", show_header=True, header_style="bold magenta")

    for col in df.columns:
        table.add_column(str(col), justify="right")

    try:
        allocation_index = df.columns.get_loc("Allocation %")
    except KeyError:
        console.print(f"[red]Error: 'Allocation %' column not found in {title}[/red]")
        return table

    for row in df.itertuples(index=False):
        try:
            percent = int(row[allocation_index])
            color = get_row_color(percent, cmap)
        except Exception:
            color = "white"

        styled_row = [f"[{color}]{cell}[/{color}]" for cell in row]
        table.add_row(*map(str, styled_row))

    return table
