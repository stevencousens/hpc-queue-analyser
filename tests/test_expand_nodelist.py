import pytest
from src.utils import expand_nodelist

def test_expand_nodelist_with_single_range():
    assert expand_nodelist("node[1-2]") == "node1,node2"

def test_expand_nodelist_with_multiple_ranges():
    assert expand_nodelist("gpu[1-3,5-7]") == "gpu1,gpu2,gpu3,gpu5,gpu6,gpu7"

def test_expand_nodelist_with_single_node():
    assert expand_nodelist("smp1") == "smp1"

def test_expand_nodelist_with_invalid_format_raises_valueerror():
    with pytest.raises(ValueError, match="Invalid nodelist format"):
        expand_nodelist("node[1")
