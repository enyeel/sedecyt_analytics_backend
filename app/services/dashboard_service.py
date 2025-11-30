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
    """
    try:
        # 1. Fetch all dashboards...
        dashboards_response = supabase_service.supabase.table('dashboards').select('*').order('position').execute()
        dashboards = dashboards_response.data
        
        # 2. Fetch all charts...
        charts_response = supabase_service.supabase.table('charts').select('*').order('position').execute()
        all_charts = charts_response.data
        
        # 3. Create a map...
        charts_by_dashboard = {}
        for chart in all_charts:
            
            # 🔥 NUEVO: FILTRO DE BACKEND
            # Si is_active es explícitamente False, ignoramos este registro.
            # Usamos .get() para que si la columna no existe aún, asuma True (visible) por defecto.
            if chart.get('is_active') is False:
                print(f"Skipping inactive chart: {chart.get('chart_slug')}")
                continue

            dashboard_id = chart['dashboard_id']
            if dashboard_id not in charts_by_dashboard:
                charts_by_dashboard[dashboard_id] = []
            
            # Reconstruct the chart object...
            charts_by_dashboard[dashboard_id].append({
                "chart_id": chart["chart_slug"],
                "title": chart["title"],
                "type": chart["chart_type"],
                "data": chart["chart_data"]
                # Ya no necesitas enviar 'is_active' al front, porque si llegó aquí, es True.
            })

        # 4. Assemble...
        for dashboard in dashboards:
            dashboard['charts'] = charts_by_dashboard.get(dashboard['id'], [])
            
        return dashboards

    except Exception as e:
        print(f"❌ Error fetching dashboards from Supabase: {e}")
        return []