"""
Configuration loader and validator for HPC Queue Analysis.

This module:
- Loads a YAML configuration file
- Validates its structure and keys
- Ensures analysis groups follow expected schema
"""

import yaml

ALLOWED_CRITERIA_KEYS = {
    "partitions", "users", "nodes", "gpu_types",
    "custom_queue_mask", "custom_capacity_mask"
}

def load_yaml(path="config.yaml"):
    """Load a YAML config file and return its contents as a dictionary."""
    try:
        with open(path) as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")

def validate_cfg(cfg):
    """Ensure config contains valid analysis groups and criteria keys."""
    
    if "analysis_groups" not in cfg:
        raise KeyError("config.yaml must contain a top-level 'analysis_groups' key")

    for idx, filt in enumerate(cfg["analysis_groups"], 1):
        if "name" not in filt:
            raise KeyError(f"analysis_groups[{idx}] is missing required key 'name'")
        if "criteria" not in filt:
            raise KeyError(f"analysis_groups[{idx}] is missing required key 'criteria'")

        # Check for unsupported criteria keys
        unknown = set(filt["criteria"]) - ALLOWED_CRITERIA_KEYS

        if unknown:
            raise ValueError(
                f"analysis_groups[{idx}] has unknown criteria keys: {unknown}"
            )
