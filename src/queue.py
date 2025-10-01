"""
Handles SLURM queue data extraction, preprocessing, and GPU assignment logic.
"""

import subprocess
import io
import pandas as pd
import shlex
from src.utils import expand_nodelist
from src.capacity_helpers import get_gpu_types, get_node_to_gpu_map, get_partition_to_gpu_map

def extract_squeue_data():
    """
    Retrieve and preprocess current SLURM queue data.

    Executes squeue in both long and short formats, which have different field options, 
    merges results, and applies preprocessing to produce a unified job queue DataFrame.
    """


    # slurm doesn't give all fields on either --Format or --format so both are needed
    cmd_long = shlex.split('squeue -r -a --Format=JobArrayID,PendingTime,tres-alloc:100')
    cmd_short = shlex.split('squeue -r -a --format=%i|%T|%r|%P|%u|%b|%N')

    raw_long = subprocess.run(cmd_long, capture_output=True, text=True).stdout
    raw_short = subprocess.run(cmd_short, capture_output=True, text=True).stdout

    df_long = pd.read_csv(io.StringIO(raw_long), sep=r'\s+').astype(str)
    df_short = pd.read_csv(io.StringIO(raw_short), sep='|').astype(str)

    return pd.merge(df_long, df_short, on='JOBID', how='outer')

def assign_gpus(row, gpu_types, node_to_gpu_map, partition_to_gpu_map):
    """Assign GPU counts to job row using node, TRES, and partition mappings."""
    gpu_total = row["gpu"]
    if gpu_total == 0:
        return row

    assigned_gpu = 0
    gpu_per_node = row["gpu_per_node"]

    # Node-level assignment (only if nodelist is present)
    for node in row.get("nodelist", []):
        gpu_type = node_to_gpu_map.get(node)
        if isinstance(gpu_type, str):
            row[gpu_type] += gpu_per_node
            assigned_gpu += gpu_per_node

    # Calculate remaining GPUs
    remaining_gpu = gpu_total - assigned_gpu
    if remaining_gpu <= 0:
        return row

    # Fallback: TRES-level assignment
    tres_type = row.get("gpu_type_tres_per_node")
    if tres_type in gpu_types:
        row[tres_type] += remaining_gpu
        return row

    # Fallback: Partition-level assignment
    part_type = partition_to_gpu_map.get(row["partition"])
    if part_type:
        row[part_type] += remaining_gpu
        return row

    # Final fallback: mark as indeterminate
    row["indeterminate_gpu"] += remaining_gpu
    return row

def preprocess_squeue_data(raw_data: str, capacities_df) -> pd.DataFrame:
    """Transform raw squeue output into enriched job DataFrame with GPU assignments."""

    gpu_types = get_gpu_types(capacities_df)
    
    
    # get node_to_gpu_map, but keep only entries where gpu is uniquely defined by node
    node_to_gpu_map = {
        node: gpus[0]
        for node, gpus in get_node_to_gpu_map(capacities_df).items()
        if len(gpus) == 1
    }

    # get partition_to_gpu_map, but keep only entries where gpu is uniquely defined by partition
    partition_to_gpu_map = {
        part: gpus[0]
        for part, gpus in get_partition_to_gpu_map(capacities_df).items()
        if len(gpus) == 1
    }


    df = (raw_data.rename(columns=str.lower)
            .assign(jobid=lambda df: df['jobid'].astype(str),
                    cpu=lambda df: df['tres_alloc'].str.extract(r'cpu=(\d+)').fillna(0).astype(int),
                    node=lambda df: df['tres_alloc'].str.extract(r'node=(\d+)').fillna(0).astype(int),
                    nodelist=lambda df: df['nodelist'].astype(str).apply(expand_nodelist).str.split(','),
                    gpu=lambda df: df['tres_alloc'].str.extract(r'gpu=(\d+)').fillna(0).astype(int),
                    gpu_per_node=lambda df: df["gpu"].div(df["node"]).fillna(0),
                    mem_gb=lambda df: df['tres_alloc'].str.extract(r'mem=(\d*\.?\d+)([KMGTP])')
                        .apply(lambda x: float(x[0]) * {'K': 1/(1000**2), 'M': 1/1000, 'G': 1, 'T': 1000}.get(x[1], 1), axis=1).round(0).astype(int),
                    gpu_type_tres_per_node=lambda df: df['tres_per_node'].str.extract(r'gpu:([^:]+)').fillna('none'),
                    pending_time=lambda df: pd.to_timedelta(pd.to_numeric(df['pending_time']),unit='s'),
                    partition_list=lambda df:df['partition'].str.split(","),
                    indeterminate_gpu=lambda df:pd.Series([0] * len(df), index=df.index),
                    reason=lambda df:df['reason'].str[:25]
                    )
            .assign(**{gpu:0 for gpu in gpu_types})
            .apply(lambda row: assign_gpus(row, gpu_types, node_to_gpu_map, partition_to_gpu_map), axis=1)
            .drop(columns=['tres_alloc','tres_per_node', 'gpu_per_node', 'gpu_type_tres_per_node']))
    return df


def get_queue_data(capacities_df):
    """Run squeue and return enriched job queue DataFrame with resource allocations."""
    raw_squeue_data = extract_squeue_data()
    preprocessed_squeue_data = preprocess_squeue_data(raw_squeue_data, capacities_df)
    return preprocessed_squeue_data

