"""
Defines the layout for the HPC queue analysis app using Textual.

Each analysis group is rendered as a tabbed pane with summary, group-by, and raw views.
"""

from textual.widgets import TabPane, TabbedContent, Markdown
from textual.containers import Horizontal, Vertical
from src.widgets import make_datatable, make_summary_datatable
from src.styles import CMAP_RUNNING, CMAP_PENDING

def compose_summary_tab(running_group, pending_group):
    """Create a tab showing summary of allocations."""
    with TabPane("📊 Summary"):
        yield Horizontal(
            Vertical(
                Markdown("# 🏃 Running Summary"),
                make_summary_datatable(running_group.summary_stats_df),
                Markdown("# Current Resource Allocation"),
                make_datatable(
                    running_group.allocation_df,
                    highlight_col="Allocation %",
                    cmap=CMAP_RUNNING
                )
            ),
            Vertical(
                Markdown("# 🕒 Pending Summary"),
                make_summary_datatable(pending_group.summary_stats_df),
                Markdown("# Pending Resource Allocation"),
                make_datatable(
                    pending_group.allocation_df,
                    highlight_col="Allocation %",
                    cmap=CMAP_PENDING
                )
            )
        )


def compose_user_allocation_tab(running_group, pending_group):
    """Create a tab showing user-level allocation stats side by side with spacing."""
    with TabPane("👥 Users"):
        yield Horizontal(
            Vertical(
                Markdown("# 🏃 Running Jobs by User"),
                make_datatable(
                    running_group.grpby_user_df.sort_values(by="cpu", ascending=False)
                )
            ),
            Vertical(
                Markdown("# 🕒 Pending Jobs by User"),
                make_datatable(
                    pending_group.grpby_user_df.sort_values(by="cpu", ascending=False)
                )
            )
        )



def compose_partition_allocation_tab(running_group, pending_group):
    """Create a tab showing resource usage grouped by partition, sorted by CPU."""
    with TabPane("📦 Partitions"):
        yield Horizontal(
            Vertical(
                Markdown("# 🏃 Running Jobs by Partition"),
                make_datatable(
                    running_group.grpby_partition_df.sort_values(by="cpu", ascending=False),
                )
            ),
            Vertical(
                Markdown("# 🕒 Pending Jobs by Partition"),
                make_datatable(
                    pending_group.grpby_partition_df.sort_values(by="cpu", ascending=False),
                )
            )
        )

def compose_queue_length_tab(pending_group):
    """Create a tab with two sub-tabs: one for Priority/Resources, one for Other reasons."""
    with TabPane("🕒 Queue Times"):
        df = pending_group.pending_time_df

        # Split into top and bottom groups
        top = df[df["reason"].isin({"Priority", "Resources"})]
        bottom = df[~df["reason"].isin({"Priority", "Resources"})]

        with TabbedContent():
            # Priority/Resources tab
            with TabPane("⏰ Priority/Resources"):
                yield Vertical(
                    Markdown("### ⏰ Priority/Resources"),
                    make_datatable(top)
                )

            # Other reasons tab
            with TabPane("🚦 Other reasons"):
                yield Vertical(
                    Markdown("### 🚦 Other reasons"),
                    make_datatable(bottom)
                )


def compose_analysis_group_tab(running_group, pending_group):
    """Create full tab layout for a pair of AnalysisGroup objects."""
    with TabPane(running_group.name):
        with TabbedContent():
            yield from compose_summary_tab(running_group, pending_group)
            yield from compose_user_allocation_tab(running_group, pending_group)
            yield from compose_partition_allocation_tab(running_group, pending_group)
            yield from compose_queue_length_tab(pending_group)
