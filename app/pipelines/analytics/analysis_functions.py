import pandas as pd

def analyze_categorical(df: pd.DataFrame, column: str, limit: int = None, label_mapping: dict = None, fill_na: str = "SIN ESPECIFICAR", **kwargs):
    """
    Analiza una columna categórica con opciones de limpieza visual.
    
    Args:
        limit: Top N resultados (ej. 10 para municipios).
        label_mapping: Diccionario para renombrar valores (ej. {True: 'Sí', False: 'No'}).
        fill_na: Texto para reemplazar valores vacíos.
    """
    if column not in df.columns:
        return None
    
    # 1. Rellenar Nulos (Para que no salgan huecos o null)
    # Convertimos a string primero para evitar problemas de tipos mixtos, 
    # excepto si es booleano que queremos mapear.
    series = df[column].fillna(fill_na)
    
    # 2. Reemplazar textos vacíos ("") que no son nulos pero están vacíos
    series = series.replace(r'^\s*$', fill_na, regex=True)

    # 3. Aplicar Mapeo (Para True/False o Tier 1/2)
    if label_mapping:
        # Map transforma los valores usando el diccionario. 
        # fillna(series) asegura que si un valor no está en el mapa, se mantenga el original.
        series = series.map(label_mapping).fillna(series)

    # 4. Contar
    counts = series.value_counts()

    # 5. Aplicar Límite (Cortar la cola larga)
    if limit:
        counts = counts.head(limit)

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

def analyze_top_ranking(df: pd.DataFrame, label_col: str, value_col: str = None, 
                        limit: int = 10, aggregation: str = 'count', 
                        filter_col: str = None, filter_value = None, **kwargs):
    """
    Genera un Top N ranking con capacidades de filtrado y agregación.
    """
    # 0. FILTRADO PREVIO (La clave para los cruces)
    if filter_col and filter_col in df.columns:
        if filter_value is True: # Para columnas booleanas
            df = df[df[filter_col] == True]
        elif filter_value is not None: # Para texto (ej. Sector = 'Automotriz')
            df = df[df[filter_col] == filter_value]
    
    if df.empty or label_col not in df.columns:
        return {"labels": [], "values": []}

    # CASO 1: Ranking por Frecuencia (Conteo simple)
    if value_col is None or aggregation == 'count':
        counts = df[label_col].value_counts().head(limit)
        return {"labels": counts.index.tolist(), "values": counts.values.tolist()}

    # CASO 2: Ranking por Suma (ej. Total de Empleados por Sector)
    if aggregation == 'sum':
        if value_col not in df.columns: return None
        # Convertimos a numérico forzosamente para evitar errores, los no numéricos a NaN y luego 0
        df[value_col] = pd.to_numeric(df[value_col], errors='coerce').fillna(0)
        
        grouped = df.groupby(label_col)[value_col].sum().sort_values(ascending=False).head(limit)
        return {"labels": grouped.index.astype(str).tolist(), "values": grouped.values.tolist()}

    # CASO 3: Raw (ya lo tenías)
    if aggregation == 'raw':
        df_sorted = df.sort_values(by=value_col, ascending=False).head(limit)
        return {"labels": df_sorted[label_col].astype(str).tolist(), "values": df_sorted[value_col].tolist()}

    return None