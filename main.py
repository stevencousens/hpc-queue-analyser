"""
Main entry point for the HPC Queue Analyser application.

This script:
- Loads and validates config file for defining analysis groups
- Retrieves capacity and queue data
- Builds analysis groups
- Launches the TUI app
"""

from src.config_loader import load_yaml, validate_cfg
from src.queue import get_queue_data
from src.capacities import get_capacities
from src.analysis_group_builder import build_analysis_group_pairs
from src.app import HPCQueueAnalyserApp
from src.cli_printer import print_analysis_group_block
import sys
import argparse


def run_stage(name, func, *args):
    """
    Runs a named stage with error handling.
    Exits the program if the function raises an exception.
    """
    try:
        return func(*args)
    except Exception as e:
        print(f"Failed to {name}: {e}")
        sys.exit(1)


if __name__ == "__main__":

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="HPC Queue Analyser")
    parser.add_argument(
        "--cli",
        action="store_true",
        help="Run in CLI mode (print summary tables) instead of launching the TUI app",
    )
    args = parser.parse_args()

    # Load and validate configuration YAML file
    config = run_stage("load config file", load_yaml)
    run_stage("validate configuration file", validate_cfg, config)

    # Load capacities and queue data (queue needs capacity data for GPU assignment)
    capacities_df = run_stage("retrieve capacity data", get_capacities)
    queue_df = run_stage("retrieve queue data", get_queue_data, capacities_df)

    # Build analysis groups (correspond to tabs in the app)
    analysis_group_pairs = run_stage(
        "build analysis group pairs",
        build_analysis_group_pairs,
        queue_df,
        capacities_df,
        config,
    )

    if args.cli:
        # CLI mode: print summaries and allocations
        for running_group, pending_group in analysis_group_pairs:
            print_analysis_group_block(running_group, pending_group)
    else:
        # Launch the app
        run_stage(
            "execute HPC queue analysis app",
            HPCQueueAnalyserApp(analysis_group_pairs).run,
        )
