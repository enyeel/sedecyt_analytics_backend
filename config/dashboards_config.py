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
            # 1. Municipios (LIMITADO a Top 10)
            {
                "slug": "companies-by-municipality",
                "data_source_key": "companies",
                "analysis_type": analyze_categorical,
                "formatter_params": {"title": "Empresas por Municipio (Top 10)", "chart_type": "bar", "data_label": "Nº de Empresas"},
                "params": {
                    "column": 'municipality',
                    "limit": 10,  # <--- NUEVO: Limita a los 10 principales
                    "fill_na": "OTRO"
                }
            },
            # 2. Sector (RELLENO de nulos)
            {
                "slug": "companies-by-sector",
                "data_source_key": "companies",
                "analysis_type": analyze_categorical,
                "formatter_params": {"title": "Empresas por Sector", "chart_type": "pie", "data_label": "Nº de Empresas"},
                "params": {
                    "column": 'sector',
                    "fill_na": "SIN SECTOR ASIGNADO" # <--- NUEVO: Etiqueta para los vacíos
                }
            },
            {
                "slug": "companies-by-procurement-tier",
                "data_source_key": "companies",
                "analysis_type": analyze_categorical,
                "formatter_params": {"title": "Empresas por Nivel de Proveeduría (Tier)", "chart_type": "bar", "data_label": "Nº de Empresas"},
                "params": {
                    "column": 'procurement_tier',
                    "fill_na": "NO ESPECIFICADO"
                }
            },
            # 3. Planes de Expansión (RENOMBRADO True/False)
            {
                "slug": "companies-by-expansion-plans",
                "data_source_key": "responses",
                "analysis_type": analyze_categorical,
                "formatter_params": {"title": "Planes de Expansión", "chart_type": "pie", "data_label": "Nº de Empresas"},
                "params": {
                    "column": 'has_expansion_plans',
                    "label_mapping": {True: "Con Planes", False: "Sin Planes", None: "No respondió"} # <--- NUEVO: Traducción
                }
            },
            {
                "slug": "companies-by-employee-count",
                "data_source_key": "companies",
                "analysis_type": analyze_continuous_binned,
                "formatter_params": {"title": "Tamaño de Empresas por Nº de Empleados", "chart_type": "bar", "data_label": "Nº de Empresas"},
                "params": {
                    "column": 'employee_count',
                    "bins": 4, 
                    "labels": ["Pequeña (Q1)", "Mediana (Q2)", "Grande (Q3)", "Muy Grande (Q4)"]
                }
            }
        ]
    },
    {
        "slug": "strategic-top-10",
        "title": "Rankings Estratégicos (Top 10)",
        "description": "Empresas y zonas destacadas por generación de empleo y densidad industrial.",
        "position": 2,
        "charts": [
            {
                "slug": "top-10-employers",
                "data_source_key": "companies",
                "analysis_type": analyze_top_ranking,
                "formatter_params": {
                    "title": "Top 10: Mayores Empleadores", 
                    "chart_type": "bar",
                    "data_label": "Empleados"
                },
                "params": {
                    "label_col": "trade_name",      
                    "value_col": "employee_count",  
                    "limit": 10,
                    "aggregation": "raw"            
                }
            },
            # --- COMENTADO TEMPORALMENTE (Esperando limpieza de datos) ---
            # {
            #     "slug": "top-10-industrial-parks",
            #     "data_source_key": "companies",
            #     "analysis_type": analyze_top_ranking,
            #     "formatter_params": {
            #         "title": "Top 10: Parques con Mayor Densidad", 
            #         "chart_type": "pie", 
            #         "data_label": "Empresas Instaladas"
            #     },
            #     "params": {
            #         "label_col": "industrial_park", 
            #         "value_col": None,              
            #         "limit": 10,
            #         "aggregation": "count"
            #     }
            # },
            # -------------------------------------------------------------
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
            },
            {
                "slug": "top-sectors-by-employment",
                "data_source_key": "companies",
                "analysis_type": analyze_top_ranking,
                "formatter_params": {
                    "title": "Top 5 Sectores Generadores de Empleo", 
                    "chart_type": "bar", # Horizontal queda genial
                    "data_label": "Total Empleados"
                },
                "params": {
                    "label_col": "sector",
                    "value_col": "employee_count",
                    "limit": 5,
                    "aggregation": "sum" 
                }
            },
            
            # CRUCE 2: Municipios vs Ingeniería (FILTRO + CONTEO)
            {
                "slug": "top-municipalities-engineering",
                "data_source_key": "responses", # Ojo: has_engineering_area suele estar en responses
                "analysis_type": analyze_top_ranking,
                "formatter_params": {
                    "title": "Top Municipios con Áreas de Ingeniería", 
                    "chart_type": "pie", 
                    "data_label": "Empresas Tecnificadas"
                },
                "params": {
                    "label_col": "municipality", # Asegúrate de tener municipality en 'responses' (vía join o copy)
                    # Si no está en responses, necesitarás hacer un merge previo en 'run_analytics_etl'
                    "filter_col": "has_engineering_area",
                    "filter_value": True,
                    "limit": 5,
                    "aggregation": "count"
                }
            },

            # CRUCE 3: Sector vs Expansión (FILTRO + CONTEO)
            {
                "slug": "top-sectors-expansion",
                "data_source_key": "responses",
                "analysis_type": analyze_top_ranking,
                "formatter_params": {
                    "title": "Sectores con Mayor Intención de Expansión", 
                    "chart_type": "bar", 
                    "data_label": "Empresas en Expansión"
                },
                "params": {
                    "label_col": "sector", 
                    "filter_col": "has_expansion_plans",
                    "filter_value": True,
                    "limit": 5,
                    "aggregation": "count"
                }
            }
        ]
    }
]