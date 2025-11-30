import os
import sys

# Truco para que Python encuentre los módulos 'app' y 'config' si ejecutas desde la raíz
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from dotenv import load_dotenv
from app.core.connections import supabase_service
from config.dashboards_config import DASHBOARDS_CONFIG

def sync_chart_visibility():
    """
    Sincroniza SOLO el estado 'is_active' de las gráficas desde el archivo de configuración
    hacia la base de datos Supabase. No recalcula datos ni toca nada más.
    """
    print("--- ⚡ Iniciando Sincronización de Visibilidad ---")
    
    updates = []
    
    # 1. Recorrer la configuración para extraer los estados
    for dashboard in DASHBOARDS_CONFIG:
        print(f"📦 Procesando dashboard: {dashboard['title']}")
        
        for chart in dashboard['charts']:
            # Extraemos slug y estado (True por defecto si no existe)
            chart_slug = chart['slug']
            is_active = chart.get('is_active', True)
            
            # Preparamos el objeto mínimo para el upsert
            updates.append({
                "chart_slug": chart_slug,
                "is_active": is_active
            })
            
            status_icon = "🟢" if is_active else "🔴"
            print(f"   {status_icon} {chart_slug}")

    # 2. Enviar actualización masiva a Supabase
    if updates:
        print(f"\n📤 Enviando {len(updates)} actualizaciones a Supabase...")
        try:
            # Upsert detectará el conflicto en 'chart_slug' y solo actualizará 'is_active'
            # (Asegúrate de que no borre el resto de la data, Supabase hace merge por defecto en upsert 
            #  si le pasas solo los campos a cambiar, PERO para estar seguros 100%, 
            #  el upsert reemplaza la fila si no especificas. 
            #  MEJOR ESTRATEGIA: Update uno por uno o Upsert con cuidado. 
            #  Supabase-py upsert reemplaza. Haremos un loop de updates para ser quirúrgicos y seguros).
            
            count = 0
            for item in updates:
                response = supabase_service.supabase.table('charts').update(
                    {'is_active': item['is_active']}
                ).eq('chart_slug', item['chart_slug']).execute()
                count += 1
                
            print(f"✅ Éxito: Se actualizaron {count} gráficas.")
            
        except Exception as e:
            print(f"❌ Error al actualizar: {e}")
    else:
        print("⚠️ No se encontraron gráficas en la configuración.")

if __name__ == '__main__':
    load_dotenv() # Carga variables de entorno (.env)
    sync_chart_visibility()