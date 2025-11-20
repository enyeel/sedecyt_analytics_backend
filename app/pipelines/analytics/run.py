import pandas as pd
import json
from app.core.connections import supabase_service
from config.dashboards_config import DASHBOARDS_CONFIG

# ==============================================================================
#  2. FORMATTING FUNCTION
#  This function takes raw data and formats it into a Chart.js object.
# ==============================================================================

def _format_chart_object(title: str, chart_type: str, data_label: str, analysis_result: dict):
    """Formats the result from an analysis function into a Chart.js compatible dictionary."""
    if not analysis_result:
        return None
    
    # This is a simplified version of your _generate_grouped_chart's formatting part
    return {
        "title": title,
        "type": chart_type,
        "data": {
            "labels": analysis_result["labels"],
            "datasets": [{"label": data_label, "data": analysis_result["values"]}],
        },
    }
 
def run_analytics_etl():
    """
    Orchestrates the entire analytics update process.
    1. Fetches clean data from Supabase.
    2. Defines the structure of dashboards and their charts.
    3. Generates chart data for each defined chart.
    4. Upserts the dashboard and chart data back to Supabase.
    """
    print("--- Starting Analytics Update Process ---")

    # --- 1. EXTRACTION: Get the clean company data ---
    print("Step 1: Fetching all required data sources...")
    data_sources = {
        'companies': pd.DataFrame(supabase_service.get_all_from('companies')),
        'responses': pd.DataFrame(supabase_service.get_all_from('responses'))
    }
    print(f"  - Fetched {len(data_sources['companies'])} company records.")
    print(f"  - Fetched {len(data_sources['responses'])} response records.")

    # --- 3. TRANSFORMATION: Generate all chart data ---
    print("\nStep 2: Generating chart data...")
    all_charts_to_upload = []

    for dashboard_config in DASHBOARDS_CONFIG:
        dashboard_id = None
        # First, ensure the dashboard exists and get its ID
        try:
            dashboard_data, _ = supabase_service.supabase.table('dashboards').upsert(
                {
                    "slug": dashboard_config["slug"],
                    "title": dashboard_config["title"],
                    "description": dashboard_config["description"],
                    "position": dashboard_config["position"]
                },
                on_conflict='slug'
            ).execute()

            dashboard_id = dashboard_data[1][0]['id']
            print(f"  - Upserted dashboard '{dashboard_config['title']}' (ID: {dashboard_id})")

        except Exception as e:
            print(f"❌ Error upserting dashboard '{dashboard_config['slug']}': {e}")
            continue

        for i, chart_config in enumerate(dashboard_config["charts"]):
            print(f"    - Generating chart: {chart_config['slug']}")
            
            # Select the correct DataFrame and analysis function from the config
            df = data_sources[chart_config["data_source_key"]]
            analysis_func = chart_config["analysis_type"]
            analysis_params = chart_config["params"]

            # Run the analysis
            analysis_result = analysis_func(df, **analysis_params)

            # Format the result into a Chart.js object
            chart_object = _format_chart_object(**chart_config["formatter_params"], analysis_result=analysis_result)

            if chart_object:
                chart_to_upload = {
                    "dashboard_id": dashboard_id,
                    "chart_slug": chart_config["slug"],
                    "title": chart_object["title"],
                    "chart_type": chart_object["type"],
                    "chart_data": chart_object["data"], # Pass the dictionary directly
                    "position": i + 1
                }
                all_charts_to_upload.append(chart_to_upload)
            else:
                print(f"    - ⚠️  Could not generate chart '{chart_config['slug']}'. Skipping.")

    # --- 4. LOAD: Upsert all generated charts to Supabase ---
    print(f"\nStep 3: Uploading {len(all_charts_to_upload)} charts to Supabase...")
    if not all_charts_to_upload:
        print("  - No charts to upload.")
    else:
        try:
            data, count = supabase_service.supabase.table('charts').upsert(
                all_charts_to_upload,
                on_conflict='chart_slug'
            ).execute()
            print(f"✅ Successfully upserted {len(data[1])} charts.")
        except Exception as e:
            print(f"❌ An error occurred during chart upload: {e}")

    print("\n--- Analytics Update Process Finished ---")

if __name__ == '__main__':
    run_analytics_etl()
