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
                        filter_col: str = None, filter_value = None, 
                        exclude_value = None, **kwargs): # <--- NUEVO PARÁMETRO
    """
    Genera un Top N ranking con capacidades de filtrado y exclusión.
    """
    # 0. FILTRADO PREVIO (La clave para los cruces)
    if filter_col and filter_col in df.columns:
        if filter_value is True: # Para columnas booleanas
            df = df[df[filter_col] == True]
        elif filter_value is not None: # Para texto (ej. Sector = 'Automotriz')
            df = df[df[filter_col] == filter_value]
    
    if exclude_value is not None:
        # Filtramos todo lo que NO SEA igual al valor excluido
        df = df[df[label_col] != exclude_value]
    
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

def analyze_array_frequency(df: pd.DataFrame, column: str, top_n: int = 10, catalog_df: pd.DataFrame = None, map_id_col: str = 'id', map_name_col: str = 'acronym', **kwargs):
    """
    1. Explota listas de IDs.
    2. Cuenta frecuencias.
    3. (Opcional) Traduce IDs a Nombres usando un catálogo.
    """
    if column not in df.columns:
        return None
    
    # 1. Limpieza y Validación de Arrays
    # A veces Pandas lee los arrays de Postgres como strings "['1', '2']". 
    # Aseguramos que sean listas reales.
    def ensure_list(x):
        if isinstance(x, list): return x
        if isinstance(x, str): 
            try:
                # Truco sucio pero efectivo para limpiar formato string de array
                clean = x.replace('{', '').replace('}', '').replace('[', '').replace(']', '').replace('"', '')
                return [i.strip() for i in clean.split(',') if i.strip()]
            except:
                return []
        return []

    # Aplicamos limpieza
    valid_rows = df.copy()
    valid_rows[column] = valid_rows[column].apply(ensure_list)
    valid_rows = valid_rows[valid_rows[column].map(len) > 0] # Filtrar vacíos

    if valid_rows.empty:
        return {"labels": [], "values": []}

    # 2. Explotar (Unnest)
    exploded_series = valid_rows[column].explode()
    
    # 3. Contar Frecuencias (Top N IDs)
    counts = exploded_series.value_counts().head(top_n)
    
    # --- FASE DE TRADUCCIÓN ---
    labels = counts.index.tolist() # Por defecto son los IDs
    
    if catalog_df is not None:
        # Crear diccionario de mapeo: { '14': 'ISO9000', '27': 'ISO9001' }
        # Convertimos a string ambos lados para asegurar match
        mapping = dict(zip(
            catalog_df[map_id_col].astype(str), 
            catalog_df[map_name_col]
        ))
        
        # Traducir los labels
        # Si no encuentra el ID, pone "ID_Desconocido"
        labels = [mapping.get(str(x), f"ID {x}") for x in labels]

    return {"labels": labels, "values": counts.values.tolist()}

def analyze_array_populated_bool(df: pd.DataFrame, column: str, true_label="Certificada", false_label="Sin Certificación", **kwargs):
    """
    Crea una métrica booleana basada en si un array/lista tiene elementos o está vacío.
    """
    if column not in df.columns: return None
    
    # Función auxiliar para checar si tiene datos
    def has_data(x):
        if isinstance(x, list): return len(x) > 0
        if isinstance(x, str): return x.strip() not in ['[]', '{}', ''] # Check básico de string vacío
        return False

    # Calculamos
    has_cert = df[column].apply(has_data)
    counts = has_cert.value_counts()
    
    # Formateamos etiquetas bonitas
    labels = [true_label if idx else false_label for idx in counts.index]
    
    return {"labels": labels, "values": counts.values.tolist()}