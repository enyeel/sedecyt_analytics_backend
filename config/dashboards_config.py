# This file defines the structure and configuration for all dynamically generated dashboards.
# The analytics_service will use this config to generate and update the data in Supabase.

from app.pipelines.analytics.analysis_functions import analyze_categorical, analyze_continuous_binned

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
    }
]