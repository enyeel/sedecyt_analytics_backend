import pandas as pd
from app.services import supabase_service
from data.inputs.mock_dashboards import MOCK_DASHBOARDS

# --- Chart Generation Functions ---

def _generate_sector_chart(companies_df: pd.DataFrame):
    """Generates a pie chart for company sectors."""
    if companies_df.empty or 'sector' not in companies_df.columns:
        return None

    sector_counts = companies_df['sector'].value_counts()

    return {
        "chart_id": "companies-by-sector",
        "title": "Empresas por Sector",
        "type": "pie",
        "data": {
            "labels": sector_counts.index.tolist(),
            "datasets": [
                {
                    "label": "Número de Empresas",
                    "data": sector_counts.values.tolist(),
                    # Add more colors if you expect more sectors
                    "backgroundColor": [
                        "rgba(255, 99, 132, 0.6)",
                        "rgba(54, 162, 235, 0.6)",
                        "rgba(255, 206, 86, 0.6)",
                        "rgba(75, 192, 192, 0.6)",
                        "rgba(153, 102, 255, 0.6)",
                    ],
                },
            ],
        },
    }

def _generate_municipality_chart(companies_df: pd.DataFrame):
    """Generates a bar chart for company municipalities."""
    if companies_df.empty or 'municipality' not in companies_df.columns:
        return None

    municipality_counts = companies_df['municipality'].value_counts()

    return {
        "chart_id": "companies-by-municipality",
        "title": "Empresas por Municipio",
        "type": "bar",
        "data": {
            "labels": municipality_counts.index.tolist(),
            "datasets": [
                {
                    "label": "Número de Empresas",
                    "data": municipality_counts.values.tolist(),
                    "backgroundColor": "rgba(54, 162, 235, 0.6)",
                },
            ],
        },
    }

# --- Main Service Function ---

def get_dashboards_with_data():
    """
    Fetches all dashboards and populates them with dynamic data where needed.
    """
    # Fetch the raw company data once
    companies_data = supabase_service.get_all_from('companies')
    companies_df = pd.DataFrame(companies_data)

    # Find the dashboard to populate
    for dashboard in MOCK_DASHBOARDS:
        if dashboard['id'] == 'companies-summary':
            # Generate and add charts if the dataframe is not empty
            if not companies_df.empty:
                dashboard['charts'] = [
                    _generate_sector_chart(companies_df),
                    _generate_municipality_chart(companies_df)
                ]
    
    return MOCK_DASHBOARDS
