import pandas as pd
import pytest

from src.queue import assign_gpus


node_to_gpu_map = {"node1":"gpu_a", "node2":"gpu_a", "node3":"gpu_b"}
partition_to_gpu_map = {"part1":"gpu_a", "part2":"gpu_b"}
gpu_types = {"gpu_a", "gpu_b"}

def test_job_with_zero_gpus_returns_all_zero_counts():

    row = pd.Series({
            "gpu":0,
            "state":"RUNNING",
            "partition":"part1",
            "gpu_per_node":1,
            "nodelist":["node1"],
            "gpu_type_tres_per_node":"none",
            "gpu_a":0,
            "gpu_b":0,
            "indeterminate_gpu":0}
            )

    out = assign_gpus(row, gpu_types, node_to_gpu_map, partition_to_gpu_map)
    
    assert out["gpu_a"] == 0
    assert out["gpu_b"] == 0
    assert out["indeterminate_gpu"] == 0

def test_single_node_single_gpu_type_counts():

    row = pd.Series({
            "gpu":2,
            "state":"RUNNING",
            "partition":"part1",
            "gpu_per_node":2,
            "nodelist":["node1"],
            "gpu_type_tres_per_node":"none",
            "gpu_a":0,
            "gpu_b":0,
            "indeterminate_gpu":0}
            )

    out = assign_gpus(row, gpu_types, node_to_gpu_map, partition_to_gpu_map)
    
    assert out["gpu_a"] == 2
    assert out["gpu_b"] == 0
    assert out["indeterminate_gpu"] == 0

def test_multi_node_same_gpu_running():

    row = pd.Series({
            "gpu":2,
            "state":"RUNNING",
            "partition":"part1",
            "gpu_per_node":1,
            "nodelist":["node1", "node2"],
            "gpu_type_tres_per_node":"none",
            "gpu_a":0,
            "gpu_b":0,
            "indeterminate_gpu":0}
            )

    out = assign_gpus(row, gpu_types, node_to_gpu_map, partition_to_gpu_map)
    
    assert out["gpu_a"] == 2
    assert out["gpu_b"] == 0
    assert out["indeterminate_gpu"] == 0

def test_multi_node_diff_gpu_running():

    row = pd.Series({
            "gpu":2,
            "state":"RUNNING",
            "partition":"part3",
            "gpu_per_node":1,
            "nodelist":["node2", "node3"],
            "gpu_type_tres_per_node":"none",
            "gpu_a":0,
            "gpu_b":0,
            "indeterminate_gpu":0}
            )

    out = assign_gpus(row, gpu_types, node_to_gpu_map, partition_to_gpu_map)
    
    assert out["gpu_a"] == 1
    assert out["gpu_b"] == 1
    assert out["indeterminate_gpu"] == 0

def test_pending_tres():

    row = pd.Series({
            "gpu":2,
            "state":"PENDING",
            "partition":"part3",
            "gpu_per_node":2,
            "nodelist":[],
            "gpu_type_tres_per_node":"gpu_a",
            "gpu_a":0,
            "gpu_b":0,
            "indeterminate_gpu":0}
            )

    out = assign_gpus(row, gpu_types, node_to_gpu_map, partition_to_gpu_map)
    
    assert out["gpu_a"] == 2
    assert out["gpu_b"] == 0
    assert out["indeterminate_gpu"] == 0

def test_pending_partition():

    row = pd.Series({
            "gpu":2,
            "state":"PENDING",
            "partition":"part1",
            "gpu_per_node":2,
            "nodelist":[],
            "gpu_type_tres_per_node":"none",
            "gpu_a":0,
            "gpu_b":0,
            "indeterminate_gpu":0}
            )

    out = assign_gpus(row, gpu_types, node_to_gpu_map, partition_to_gpu_map)
    
    assert out["gpu_a"] == 2
    assert out["gpu_b"] == 0
    assert out["indeterminate_gpu"] == 0

def test_indeterminate():

    row = pd.Series({
            "gpu":2,
            "state":"PENDING",
            "partition":"part3",
            "gpu_per_node":2,
            "nodelist":[],
            "gpu_type_tres_per_node":"none",
            "gpu_a":0,
            "gpu_b":0,
            "indeterminate_gpu":0}
            )

    out = assign_gpus(row, gpu_types, node_to_gpu_map, partition_to_gpu_map)
    
    assert out["gpu_a"] == 0
    assert out["gpu_b"] == 0
    assert out["indeterminate_gpu"] == 2
