"""
Defines the AnalysisGroup class, which encapsulates a filtered view of the
HPC job queue and associated capacity data.

Each AnalysisGroup instance represents one analysis group (e.g. a partition,
GPU type, or user subset) and provides:

- summary statistics (users, jobs, median pending time)
- resource allocation vs. capacity
- breakdowns by user and partition
- pending time analysis by partition and reason

These precomputed DataFrames are used by both the CLI and TUI layers to
render tables and visualisations of cluster utilisation.
"""

import pandas as pd

class AnalysisGroup:
    def __init__(self,name,queue,capacity):
        self.name = name
        self.queue = queue
        self.capacity = capacity[capacity != 0]
        self.resource_list = list(self.capacity.index)
    
        self.summary_stats_df = self._compute_summary_stats_df()
        self.allocation_df = self._compute_allocation_df()
        self.grpby_user_df = self._compute_user_allocation_df()
        self.grpby_partition_df = self._compute_partition_allocation_df()
        self.pending_time_df = self._compute_pending_time_df()

    def _compute_summary_stats_df(self) -> pd.DataFrame:
        nunique_users = self.queue['user'].nunique()
        nunique_jobs = self.queue['jobid'].nunique()
        median = self.queue["pending_time"].median()
        median_pending_time = "N/A" if pd.isna(median) else median.floor("s")

        return pd.DataFrame({
            "Metric": ["Users", "Jobs", "Pending Time (Median)"],
            "Value": [nunique_users, nunique_jobs, median_pending_time]
        })
    
    def _compute_allocation_df(self) -> pd.DataFrame:
        allocation = self.queue[self.resource_list].sum().round().astype(int)
        capacity = self.capacity.round().astype(int)
        allocation_pc = allocation.div(capacity).mul(100).round().astype(int)

        return pd.DataFrame({
            "Resource": self.resource_list,
            "Allocation": allocation.values,
            "Capacity": capacity.values,
            "Allocation %": allocation_pc.values
        })

    def _compute_user_allocation_df(self):
        return self.compute_allocation_summary_df("user")

    def _compute_partition_allocation_df(self):
        return self.compute_allocation_summary_df("partition")

    def compute_allocation_summary_df(self, groupby_col: str) -> pd.DataFrame:
        """Compute resource allocation summary grouped by a given column (e.g. 'user', 'partition')."""

        agg_dict = {
            "jobs": ("jobid", "count"),
            **{res: (res, "sum") for res in self.resource_list}
        }

        cap = self.capacity.loc[self.resource_list]
        gpu_cols = [res for res in self.resource_list if res not in {"cpu", "mem_gb"}]

        def summarize_gpus(row):
            used = {gpu: row[gpu] for gpu in gpu_cols if row[gpu] > 0}
            return ", ".join(f"{gpu}: {count}" for gpu, count in used.items()) if used else "—"

        return (
            self.queue
            .groupby(groupby_col)
            .agg(**agg_dict)
            .pipe(lambda df: df.assign(**{res: df[res].round().astype(int) for res in self.resource_list}))
            .pipe(lambda df: df.assign(
                **{f"{res} %": df[res].div(cap[res]).mul(100).round().astype(int) for res in ["cpu", "mem_gb"]}
            ))
            .assign(gpu=lambda df: df.apply(summarize_gpus, axis=1))
            .drop(columns=gpu_cols)
            .reset_index()
            .loc[:, [groupby_col, "jobs", "cpu", "cpu %", "mem_gb", "mem_gb %", "gpu"]]
        )

    def _compute_pending_time_df(self) -> pd.DataFrame:
        """Compute job counts, median pending time, and median resource requests grouped by partition and reason."""

        df = self.queue.copy()
        df = df[df["state"] == "PENDING"]

        # Group and aggregate
        agg_dict = {
            "jobs": ("jobid", "count"),
            "median pending time": ("pending_time", "median"),
            **{res: (res, "sum") for res in self.resource_list}
        }

        grouped = df.groupby(["partition", "reason"]).agg(**agg_dict)

        # Format time and round resources
        grouped["median pending time"] = grouped["median pending time"].dt.floor("s")
        for res in self.resource_list:
            grouped[res] = grouped[res].round().astype(int)

        grouped = grouped.reset_index()

        # Prioritize key reasons
        priority_reasons = {"Priority", "Resources"}
        top = grouped[grouped["reason"].isin(priority_reasons)]
        bottom = grouped[~grouped["reason"].isin(priority_reasons)]

        return pd.concat([top, bottom], ignore_index=True)
