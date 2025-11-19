import pandas as pd

def analyze_categorical(df: pd.DataFrame, column: str, **kwargs):
    """Analyzes a categorical column by counting values."""
    if column not in df.columns:
        return None
    counts = df[column].value_counts()
    return {"labels": counts.index.tolist(), "values": counts.values.tolist()}

def analyze_continuous_binned(df: pd.DataFrame, column: str, bins: int = 4, labels: list = None, **kwargs):
    """Analyzes a continuous column by binning it into ranges."""
    if column not in df.columns:
        return None
    try:
        binned_data = pd.qcut(df[column], q=bins, labels=labels, duplicates='drop')
        counts = binned_data.value_counts().sort_index()
        return {"labels": counts.index.astype(str).tolist(), "values": counts.values.tolist()}
    except Exception as e:
        print(f"  - ⚠️  Could not bin column '{column}': {e}")
        return None