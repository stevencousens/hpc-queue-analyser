"""
Utility functions for building Textual widgets used in the HPC Queue Analyser app.

Includes Markdown summaries, color-coded tables, and DataFrame renderers.
"""

from textual.widgets import DataTable
from rich.text import Text
import pandas as pd


def get_row_color(value: int, cmap: dict) -> str:
    """Return color based on allocation percentage using a threshold map."""
    for threshold in sorted(cmap.keys(), reverse=True):
        if value >= threshold:
            return cmap[threshold]
    return "white"

def make_summary_datatable(df: pd.DataFrame) -> DataTable:
    """Render summary stats as a DataTable with emoji + metric/value."""
    emoji_map = {
        "Users": "👥",
        "Jobs": "🔧", 
        "Pending Time (Median)": "⏳",
    }

    table = DataTable(zebra_stripes=False)
    table.add_column(" ", width=2)          # emoji column
    table.add_column("Metric / Value")      # text column

    for metric, value in zip(df["Metric"], df["Value"]):
        icon = emoji_map.get(metric, "")
        table.add_row(icon, f"{metric}: {value}")

    table.cursor_type = None
    table.show_header = False
    return table

def make_datatable(data, highlight_col: str = None, cmap: dict = None) -> DataTable:
    """Convert a pandas DataFrame or Series into a Textual DataTable with optional row highlighting."""
    table = DataTable()

    df = data.to_frame().T if isinstance(data, pd.Series) else data.reset_index(drop=True)

    table.add_columns(*df.columns.astype(str).tolist())

    # Helper to wrap a cell in a Text object with correct justification and style
    def format_cell(cell, dtype, style=""):
        if pd.isna(cell):
            text = "—"
        elif pd.api.types.is_float_dtype(dtype):
            text = f"{cell:,.2f}"   # format floats nicely
        elif pd.api.types.is_integer_dtype(dtype):
            text = f"{cell:,}"      # add thousands separator
        else:
            text = str(cell)

        justify = "right" if pd.api.types.is_numeric_dtype(dtype) else "left"
        return Text(text, style=style, justify=justify)

    for row in df.itertuples(index=False, name=None):
        row_style = None
        if highlight_col and cmap and highlight_col in df.columns:
            try:
                idx = df.columns.get_loc(highlight_col)
                row_style = get_row_color(int(row[idx]), cmap)
            except Exception:
                pass

        styled_row = [
            format_cell(cell, dtype, style=row_style or "")
            for cell, dtype in zip(row, df.dtypes)
        ]
        table.add_row(*styled_row)

    return table


