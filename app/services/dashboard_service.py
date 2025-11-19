from app.core.connections import supabase_service

def get_dashboards_with_data():
    """
    Fetches all dashboards and their pre-calculated charts from Supabase.
    This is a lightweight operation designed to be called by the API.
    """
    try:
        # 1. Fetch all dashboards, ordered by position
        dashboards_response = supabase_service.supabase.table('dashboards').select('*').order('position').execute()
        dashboards = dashboards_response.data

        # 2. Fetch all charts, ordered by position
        charts_response = supabase_service.supabase.table('charts').select('*').order('position').execute()
        all_charts = charts_response.data

        # 3. Create a map of dashboard_id -> list of charts for easy lookup
        charts_by_dashboard = {}
        for chart in all_charts:
            dashboard_id = chart['dashboard_id']
            if dashboard_id not in charts_by_dashboard:
                charts_by_dashboard[dashboard_id] = []
            
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

        return dashboards
    except Exception as e:
        print(f"❌ Error fetching dashboards from Supabase: {e}")
        return []