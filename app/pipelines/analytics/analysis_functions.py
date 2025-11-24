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
    
def analyze_top_ranking(df: pd.DataFrame, label_col: str, value_col: str = None, limit: int = 10, aggregation: str = 'count', **kwargs):
    """
    Genera un Top N ranking.
    
    Args:
        label_col: Columna para las etiquetas (ej. 'trade_name', 'industrial_park').
        value_col: Columna numérica para ordenar (ej. 'employee_count'). Si es None, usa conteo.
        limit: Número máximo de resultados (Top 5, Top 10).
        aggregation: 'count' (frecuencia) o 'sum'/'max' (si se agrupan datos).
                     Si value_col existe y no queremos agrupar (ej. lista de empresas), usamos 'raw'.
    """
    if label_col not in df.columns:
        return None

    # CASO 1: Ranking por Frecuencia (ej. Top Parques Industriales)
    if value_col is None or aggregation == 'count':
        counts = df[label_col].value_counts().head(limit)
        return {
            "labels": counts.index.tolist(),
            "values": counts.values.tolist()
        }

    # CASO 2: Ranking por Valor Numérico Directo (ej. Top Empleadores)
    # Asumimos que queremos ver los valores tal cual están en la tabla (sin agrupar)
    if aggregation == 'raw':
        if value_col not in df.columns: return None
        
        # Ordenamos y cortamos
        df_sorted = df.sort_values(by=value_col, ascending=False).head(limit)
        return {
            "labels": df_sorted[label_col].astype(str).tolist(),
            "values": df_sorted[value_col].fillna(0).tolist()
        }
    
    # CASO 3: Ranking Agrupado por Suma (ej. Top Municipios por Total de Empleados)
    if aggregation == 'sum':
        if value_col not in df.columns: return None
        
        grouped = df.groupby(label_col)[value_col].sum().sort_values(ascending=False).head(limit)
        return {
            "labels": grouped.index.astype(str).tolist(),
            "values": grouped.values.tolist()
        }

    return None