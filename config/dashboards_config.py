# This file defines the structure and configuration for all dynamically generated dashboards.
# The analytics_service will use this config to generate and update the data in Supabase.

from app.pipelines.analytics.analysis_functions import analyze_categorical, analyze_continuous_binned, analyze_top_ranking

DASHBOARDS_CONFIG = [
    {
        "slug": "companies-summary",
        "title": "Análisis de Empresas",
        "description": "Distribución de empresas registradas por sector, municipio y otros indicadores clave.",
        "position": 1,
        "charts": [
            # --- Categorical Charts ---
            {
                "slug": "companies-by-municipality",
                "data_source_key": "companies",
                "analysis_type": analyze_categorical,
                "formatter_params": {"title": "Empresas por Municipio", "chart_type": "bar", "data_label": "Nº de Empresas"},
                "params": {
                    "column": 'municipality'
                }
            },
            {
                "slug": "companies-by-sector",
                "data_source_key": "companies",
                "analysis_type": analyze_categorical,
                "formatter_params": {"title": "Empresas por Sector", "chart_type": "pie", "data_label": "Nº de Empresas"},
                "params": {
                    "column": 'sector'
                }
            },
            {
                "slug": "companies-by-procurement-tier",
                "data_source_key": "companies",
                "analysis_type": analyze_categorical,
                "formatter_params": {"title": "Empresas por Nivel de Proveeduría (Tier)", "chart_type": "bar", "data_label": "Nº de Empresas"},
                "params": {
                    "column": 'procurement_tier'
                }
            },
            {
                "slug": "companies-by-expansion-plans",
                "data_source_key": "responses", # Corrected: This data is in the responses table
                "analysis_type": analyze_categorical,
                "formatter_params": {"title": "Planes de Expansión", "chart_type": "pie", "data_label": "Nº de Empresas"},
                "params": {
                    "column": 'has_expansion_plans'
                }
            },
            # --- Continuous Binned Chart ---
            {
                "slug": "companies-by-employee-count",
                "data_source_key": "companies",
                "analysis_type": analyze_continuous_binned,
                "formatter_params": {"title": "Tamaño de Empresas por Nº de Empleados", "chart_type": "bar", "data_label": "Nº de Empresas"},
                "params": {
                    "column": 'employee_count',
                    "bins": 4, # We want 4 bins (quartiles)
                    "labels": ["Pequeña (Q1)", "Mediana (Q2)", "Grande (Q3)", "Muy Grande (Q4)"]
                }
            }
        ]
    },
    {
        "slug": "strategic-top-10",
        "title": "Rankings Estratégicos (Top 10)",
        "description": "Empresas y zonas destacadas por generación de empleo y densidad industrial.",
        "position": 2, # Para que salga después del Resumen
        "charts": [
            # 1. Top 10 Generadores de Empleo (Ranking directo)
            {
                "slug": "top-10-employers",
                "data_source_key": "companies",
                "analysis_type": analyze_top_ranking,
                "formatter_params": {
                    "title": "Top 10: Mayores Empleadores", 
                    "chart_type": "bar", # Bar chart horizontal queda mejor para rankings
                    "data_label": "Empleados"
                },
                "params": {
                    "label_col": "trade_name",      # Etiqueta: Nombre de la empresa
                    "value_col": "employee_count",  # Valor: Número de empleados
                    "limit": 10,
                    "aggregation": "raw"            # Queremos el valor directo, no contar
                }
            },
            # 2. Top 10 Parques Industriales (Ranking por Frecuencia/Densidad)
            {
                "slug": "top-10-industrial-parks",
                "data_source_key": "companies",
                "analysis_type": analyze_top_ranking,
                "formatter_params": {
                    "title": "Top 10: Parques con Mayor Densidad", 
                    "chart_type": "pie", 
                    "data_label": "Empresas Instaladas"
                },
                "params": {
                    "label_col": "industrial_park", # Agrupamos por Parque
                    "value_col": None,              # No hay columna de valor, es conteo
                    "limit": 10,
                    "aggregation": "count"
                }
            },
            # 3. Top Municipios por Fuerza Laboral (Ranking Agrupado - Suma)
            # Responde: ¿Dónde está la masa laboral más grande?
            {
                "slug": "top-municipalities-workforce",
                "data_source_key": "companies",
                "analysis_type": analyze_top_ranking,
                "formatter_params": {
                    "title": "Top Municipios por Fuerza Laboral Total", 
                    "chart_type": "bar", 
                    "data_label": "Total de Empleados Registrados"
                },
                "params": {
                    "label_col": "municipality",    # Agrupar por Municipio
                    "value_col": "employee_count",  # Sumar empleados
                    "limit": 5,                     # Solo el Top 5
                    "aggregation": "sum"            # Sumar todos los empleados de las empresas en ese municipio
                }
            }
        ]
    }
]