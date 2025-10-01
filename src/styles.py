"""
Colour maps for allocation percentages used in CLI and TUI rendering.

CMAP_RUNNING: high utilisation is good (green).
CMAP_PENDING: high utilisation is bad (red).
"""

# Color maps for allocation percentages

CMAP_RUNNING = {
    0: "red",
    25: "orange1",
    50: "yellow",
    75: "green"
}

CMAP_PENDING = {
    0: "green",
    25: "yellow",
    50: "orange1",
    75: "red"
}

