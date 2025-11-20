from app.core.connections import supabase_service

def get_all_dashboards_list():
    """
    Fetches a lightweight list of all dashboards from Supabase, without chart data.
    """
    try:
        # Fetch all dashboards, ordered by position, but exclude the chart data
        dashboards_response = supabase_service.supabase.table('dashboards').select('id, slug, title, description, position').order('position').execute()
        return dashboards_response.data
    except Exception as e:
        print(f"❌ Error fetching dashboard list from Supabase: {e}")
        return []


def get_dashboards_with_data():
    """
    Fetches all dashboards and their pre-calculated charts from Supabase.
    This is a lightweight operation designed to be called by the API.
    """
    try:
        # 1. Fetch all dashboards, ordered by position
        dashboards_response = supabase_service.supabase.table('dashboards').select('*').order('position').execute()
        dashboards = dashboards_response.data
        print(f"DEBUG: Fetched {len(dashboards)} dashboards from DB.")
        # print(f"DEBUG: Raw dashboards: {dashboards}") # Uncomment for extreme detail

        # 2. Fetch all charts, ordered by position
        charts_response = supabase_service.supabase.table('charts').select('*').order('position').execute()
        all_charts = charts_response.data
        print(f"DEBUG: Fetched {len(all_charts)} charts from DB.")
        # print(f"DEBUG: Raw charts: {all_charts}") # Uncomment for extreme detail

        # 3. Create a map of dashboard_id -> list of charts for easy lookup
        charts_by_dashboard = {}
        for chart in all_charts:
            dashboard_id = chart['dashboard_id']
            if dashboard_id not in charts_by_dashboard:
                charts_by_dashboard[dashboard_id] = []
            print(f"DEBUG: Mapping chart '{chart['chart_slug']}' to dashboard_id '{dashboard_id}'")
            
            # Reconstruct the chart object for the frontend
            charts_by_dashboard[dashboard_id].append({
                "chart_id": chart["chart_slug"],
                "title": chart["title"],
                "type": chart["chart_type"],
                "data": chart["chart_data"] # Supabase client auto-parses JSONB
            })

        # 4. Assemble the final response
        for dashboard in dashboards:
            dashboard['charts'] = charts_by_dashboard.get(dashboard['id'], [])
            print(f"DEBUG: Assembled dashboard '{dashboard['slug']}' with {len(dashboard['charts'])} charts.")

        print("--- DEBUG: FINAL ASSEMBLED OBJECT (first item) ---")
        print(dashboards[0] if dashboards else "No dashboards found")
        return dashboards
    except Exception as e:
        print(f"❌ Error fetching dashboards from Supabase: {e}")
        return []