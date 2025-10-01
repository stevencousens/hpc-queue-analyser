"""
Launches the Textual UI for HPC queue analysis using tabbed views.
"""

from textual.app import App, ComposeResult
from textual.widgets import TabbedContent
from textual.binding import Binding
from textual.widgets import DataTable

from src.layout import compose_analysis_group_tab
from typing import Sequence


class HPCQueueAnalyserApp(App):
    """Textual app for visualizing HPC queue analysis.

    Each tab displays filtered job and capacity data for an analysis group.
    """
    BINDINGS = [
        Binding("q", "quit", "Quit the app"),
    ]

    def __init__(self, analysis_groups: Sequence, **kwargs):
        super().__init__(**kwargs)
        self.analysis_groups = analysis_groups

    def compose(self) -> ComposeResult:
        with TabbedContent():
            for running_group, pending_group in self.analysis_groups:
                yield from compose_analysis_group_tab(running_group, pending_group)


