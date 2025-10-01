import subprocess
import io
import pandas as pd
import re
import shlex

def extract_capacity_data() -> io.StringIO:
    """Run `sinfo` and return cleaned node capacity data as a stream."""
    cmd = shlex.split('sinfo -a --format=%N|%P|%c|%m|%G -N')
    raw_output = subprocess.run(cmd, capture_output=True, text=True).stdout

    # Remove (S:...) slot ranges and '*' flags
    cleaned = re.sub(r'\(S:[^)]*\)', '', raw_output)
    cleaned = cleaned.replace('*', '')

    return io.StringIO(cleaned)

def process_capacity_data(raw_data_file) -> pd.DataFrame:
    """
    Parse node capacity data and extract GPU counts into a DataFrame.
    Adds one column per GPU type with integer counts.
    """
# Read and normalise basics
    df = (pd.read_csv(raw_data_file, sep='|', skipinitialspace=True, dtype={'GRES': str})
            .rename(columns={
                'NODELIST': 'node',
                'PARTITION': 'partition',
                'CPUS': 'cpu',
                'MEMORY': 'mem_mb',
                'GRES': 'gres'
            })
            .assign(mem_gb=lambda d: pd.to_numeric(d['mem_mb'], errors='coerce') / 1000))

    # Pattern for gpu type and count
    pattern = r'gpu:(?P<gpu_type>[^:,(]+):(?P<gpu_count>\d+)'

    # Extract all gpu_type/gpu_count matches; keep MultiIndex from extractall
    matches = df['gres'].fillna('').str.extractall(pattern)
    matches['gpu_count'] = matches['gpu_count'].astype(int)

    if matches.empty:
        return df.drop(columns=['mem_mb', 'gres']).copy()

    # Group by original row index (level=0 of MultiIndex) and gpu_type
    gpu_counts = (matches
                  .groupby([matches.index.get_level_values(0), 'gpu_type'])['gpu_count']
                  .sum()
                  .unstack(fill_value=0)
                  .sort_index(axis=1))

    # Merge back and ensure int dtype for GPU columns
    df = df.drop(columns=['mem_mb', 'gres']).join(gpu_counts).fillna(0)
    df[gpu_counts.columns] = df[gpu_counts.columns].astype(int)

    return df

def get_capacities() -> pd.DataFrame:
    """Return processed Slurm node capacity data as a DataFrame."""
    raw_capacity_data = extract_capacity_data()
    processed_capacity_data = process_capacity_data(raw_capacity_data)
    return processed_capacity_data
