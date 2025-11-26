# debug_map.py
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

ACCENT_MAP = str.maketrans("ÁÉÍÓÚÜÑ", "AEIOUUN")

def debug_id_11():
    print("--- 🕵️‍♂️ INVESTIGANDO AL ID 11 ---")
    
    # 1. Ver qué hay CRUDO en Supabase
    response = supabase.table('municipality_catalog').select('*').eq('id', 11).execute()
    data = response.data
    
    if not data:
        print("❌ ¡PÁNICO! El ID 11 no existe en Supabase.")
        return

    raw_name = data[0]['municipality_name']
    keywords = data[0]['keywords']
    
    print(f"1. Nombre Crudo en BD: '{raw_name}'")
    print(f"   - Longitud: {len(raw_name)}")
    print(f"   - Caracteres: {[ord(c) for c in raw_name]}") # Esto revela caracteres invisibles
    
    print(f"2. Keywords Crudos: {keywords}")
    if keywords:
        print(f"   - Tipo de dato: {type(keywords)}")

    # 2. Simular tu limpieza
    clean_name = str(raw_name).upper().strip().translate(ACCENT_MAP)
    clean_name = clean_name.replace('.', '').replace(',', '').replace('/', '')
    
    print(f"3. Así queda la LLAVE OFICIAL tras limpieza: '{clean_name}'")
    
    # 3. Simular el Input problemático
    input_problematico = "SAN FRANCISCO DE LOS ROMO"
    print(f"4. Comparando con Input esperado: '{input_problematico}'")
    
    if clean_name == input_problematico:
        print("   ✅ ¡MATCH EXACTO! (El código debería funcionar)")
    else:
        print("   ❌ NO HAY MATCH EXACTO. Diferencia encontrada.")

if __name__ == "__main__":
    debug_id_11()