"""
Builds analysis groups from queue and capacity data using configurable filters.
"""

import pandas as pd
from src.analysis_group import AnalysisGroup


def _apply_partition_filter(df, partitions):
    """Filter rows by partition name or list."""
    if not partitions or partitions == "*":
        return pd.Series(True, index=df.index)
    partitions = set(partitions)
    if "partition_list" in df.columns:
        return df["partition_list"].apply(lambda lst: bool(partitions & set(lst)))
    return df["partition"].isin(partitions)


def _apply_user_filter(df, users):
    """Filter rows by user name."""
    if not users or users == "*":
        return pd.Series(True, index=df.index)
    return df["user"].isin(users)


def _apply_gpu_filter(df, gpu_types):
    """Filter rows by presence of specified GPU types."""
    if not gpu_types or gpu_types == "*":
        return pd.Series(True, index=df.index)
    return (df[gpu_types] > 0).any(axis=1)


def _apply_node_filter(df, nodes):
    """Filter rows by node name or list."""
    if not nodes or nodes == "*":
        return pd.Series(True, index=df.index)
    if "nodelist" in df.columns:
        return df["nodelist"].apply(lambda lst: bool(set(nodes) & set(lst)))
    return df["node"].isin(nodes)


def _apply_custom_filter(df, mask_expr, context_name):
    """Apply custom mask expression or callable to DataFrame."""
    if not mask_expr or mask_expr == "*":
        return pd.Series(True, index=df.index)
    if callable(mask_expr):
        return mask_expr(df)
    try:
        # Restrict globals for eval to avoid unsafe execution
        return eval(mask_expr, {context_name: df, "__builtins__": {}})
    except Exception as e:
        print(f"Error evaluating {context_name} mask: {e}")
        return pd.Series(True, index=df.index)


def build_analysis_group_pairs(queue: pd.DataFrame, capacity: pd.DataFrame, config: dict):
    """
    Build paired AnalysisGroup objects for RUNNING and PENDING jobs based on configured filters.

    This function applies partition, user, GPU, node, and custom filters to both the job queue and 
    capacity data. For each analysis group defined in the config, it creates two AnalysisGroup instances:
    one containing only RUNNING jobs, and one containing only PENDING jobs. These are returned as tuples.

    Args:
        queue (pd.DataFrame): The full job queue dataset.
        capacity (pd.DataFrame): The resource capacity dataset.
        config (dict): Configuration dictionary specifying analysis group criteria.

    Returns:
        List[Tuple[AnalysisGroup, AnalysisGroup]]: A list of (running_group, pending_group) pairs.
    """



    analysis_group_pairs = []

    for ag in config.get("analysis_groups", []):
        name = ag["name"]
        criteria = ag.get("criteria", {})

        qmask = (
            _apply_partition_filter(queue, criteria.get("partitions"))
            & _apply_user_filter(queue, criteria.get("users"))
            & _apply_gpu_filter(queue, criteria.get("gpu_types"))
            & _apply_node_filter(queue, criteria.get("nodes"))
            & _apply_custom_filter(queue, criteria.get("custom_queue_mask"), "queue")
        )

        cmask = (
            _apply_partition_filter(capacity, criteria.get("partitions"))
            & _apply_gpu_filter(capacity, criteria.get("gpu_types"))
            & _apply_node_filter(capacity, criteria.get("nodes"))
            & _apply_custom_filter(capacity, criteria.get("custom_capacity_mask"), "capacity")
        )

        queue_slice = queue.loc[qmask]

        capacity_slice = (
            capacity.loc[cmask]
            .drop_duplicates("node")
            .drop(columns=["node", "partition"], errors="ignore")
            .sum()
        )
        running_group = AnalysisGroup(name, queue_slice[queue_slice["state"] == "RUNNING"], capacity_slice)
        pending_group = AnalysisGroup(name, queue_slice[queue_slice["state"] == "PENDING"], capacity_slice)

        analysis_group_pairs.append((running_group, pending_group))


    return analysis_group_pairs
