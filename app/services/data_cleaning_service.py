import re
import pandas as pd
from typing import Union, List

# Patrón para identificar caracteres no alfanuméricos que queremos eliminar (como guiones, espacios, etc.)
ALPHANUM_PATTERN = re.compile(r'[^a-zA-Z0-9]') 
# Patrón para eliminar sufijos legales
LEGAL_SUFFIX_PATTERN = re.compile(r'\s+(S\.\s*A\.\s*(DE\s*C\.\s*V\.|CV)?|S\.\s*C|S\.\s*R\.\s*L|ASOCIACION CIVIL|A\.C\.)\s*', re.IGNORECASE)


# --- FUNCIONES CRÍTICAS DE NORMALIZACIÓN (Nivel 1) ---

def clean_rfc(text: Union[str, float]) -> str:
    """
    Limpia y valida RFC mexicano (12 o 13 caracteres) o retiene IDs extranjeros.
    """
    if pd.isna(text) or str(text).strip() == '':
        return 'ID_FALTA' # Valor por defecto, ver punto 3.
        
    cleaned = str(text).upper().strip()
    
    # 1. Limpieza general: quitar espacios/guiones, mantener letras/números/&
    cleaned = re.sub(r'[^\w\s&Ñ]', '', cleaned)
    cleaned = re.sub(r'\s+', '', cleaned).strip()
    
    # 2. Validación de RFC Mexicano
    # Patrón: 3-4 letras/&/Ñ + 6 dígitos + 3 caracteres (Homoclave)
    if re.fullmatch(r'[A-Z&Ñ]{3,4}\d{6}[A-Z0-9]{3}', cleaned):
        return cleaned
    
    # 3. Identificador Extranjero (Numérico)
    # Si tiene solo dígitos y tiene una longitud típica de Tax ID (ej. 9-15 dígitos)
    if cleaned.isdigit() and len(cleaned) >= 8 and len(cleaned) <= 15:
        # Lo marcamos para saber que es un ID extranjero
        return f'ID_EXT_{cleaned}'
        
    # 4. Fallo: el texto no es RFC mexicano ni ID numérico típico
    return f'ID_FALLO_{cleaned[:15]}'

def clean_email(text: Union[str, float]) -> str:
    """
    Limpia el email: minúsculas y elimina espacios.
    """
    if pd.isna(text):
        return ''
    return str(text).lower().strip()

def clean_phone_to_e164(text: Union[str, float]) -> str:
    """
    Limpia y estandariza el número telefónico al formato E.164 (+<código país><número>).
    Prioriza números que ya tienen código de país. Asume +52 para 10 dígitos sin prefijo.
    """
    if pd.isna(text) or str(text).strip() == '':
        return ''
    
    raw_text = str(text).strip()
    
    # 1. Verificar si el número original empieza con '+'. Esto es la clave para internacionales.
    starts_with_plus = raw_text.startswith('+')
    
    # 2. Eliminar todo excepto dígitos
    digits = re.sub(r'\D', '', raw_text)
    
    # --- Lógica de Prioridad ---
    
    # CASO 1: Ya tenía un código de país (+)
    if starts_with_plus:
        # Si tiene un '+' pero es muy corto, es inválido.
        if len(digits) < 8:
            return ''
            
        # El número ya está en formato internacional, solo lo estandarizamos.
        # Esto soluciona tu problema con los números portugueses (+351...).
        return f'+{digits}'

    # CASO 2: México (10 dígitos sin prefijo)
    elif len(digits) == 10:
        # Si tiene 10 dígitos, asumimos México y forzamos el prefijo +52
        return f'+52{digits}'

    # CASO 3: México (Empezó con 52 o 521, pero sin el '+')
    elif len(digits) >= 11 and digits.startswith('52'):
        
        if len(digits) == 12 and digits.startswith('521'):
            # Formato 521XXXXXXXXXX (12 digitos). Quitamos el 1 de móvil.
            return f'+52{digits[3:]}'
        elif len(digits) == 12 and digits.startswith('52'):
            # Formato 52XXXXXXXXXX (12 digitos). Quitamos el 52 (2 digitos).
            return f'+52{digits[2:]}'
        # Para otros casos que empiezan con 52, retornamos el número completo con el '+'
        return f'+{digits}' 

    # CASO 4: Número no identificable
    else:
        # Si tiene menos de 7 dígitos o es una cadena extraña
        return ''

def clean_company_name(text: Union[str, float]) -> str:
    """
    Normaliza el nombre de la empresa: Mayúsculas y elimina sufijos legales comunes.
    """
    if pd.isna(text):
        return ''
        
    cleaned = str(text).upper().strip()
    
    # 1. Eliminar sufijos legales con RegEx
    cleaned = LEGAL_SUFFIX_PATTERN.sub('', cleaned)
    
    # 2. Eliminar puntuación y espacios extra
    cleaned = re.sub(r'[\.,]', '', cleaned).strip()
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    return cleaned
    

# --- FUNCIONES DE FORMATO Y TIPO (Nivel 2) ---

def clean_contact_name(text: Union[str, float]) -> str:
    """
    Limpia nombres y apellidos: elimina puntuación, reduce espacios múltiples y aplica Title Case.
    """
    
    if pd.isna(text) or text is None:
        return ""
    
    text = str(text).strip()
    
    # 1. Eliminar puntuación común que no sea esencial (ej. puntos, comas, guiones)
    # Esto corrige: "Fernando .", "Horacio Valenzuela B.,Valenzuela Bracamontes" (el punto final)
    text = re.sub(r'[.,;]', '', text) 
    
    # 2. Reducir múltiples espacios (incluye espacios Unicode como el non-breaking space ' ')
    # Esto corrige: "Jeanette  Medina", "Joel  Mata"
    text = re.sub(r'\s+', ' ', text)
    
    # 3. Aplicar Title Case o Upper, dependiendo de tu estándar
    # Usaremos Title Case para nombres propios: "juan perez" -> "Juan Perez"
    return text.title()

def clean_to_integer(text: Union[str, float]) -> Union[int, None]:
    """
    Convierte el texto a un número entero (int), eliminando caracteres no numéricos
    y aplicando la lógica de negocio para 'Número de empleados'.
    
    Regla: Si el valor final es 0, se considera como dato faltante y se retorna None.
    """
    
    # 1. Manejo inicial de valores faltantes (NaN, None, string vacío)
    if pd.isna(text) or str(text).strip() == '':
        return None
        
    # 2. Limpieza de caracteres: Elimina todo lo que no sea un dígito
    # Esto manejará '+ 380' -> '380', '10-20' -> '1020' (si no se maneja el rango antes)
    cleaned_digits = re.sub(r'\D', '', str(text).strip())
    
    # Si después de la limpieza no queda nada (ej. 'N/A' -> '')
    if not cleaned_digits:
        return None 

    # 3. Conversión y Lógica de Negocio
    try:
        final_integer = int(cleaned_digits)
        
        # LÓGICA CLAVE: Si el resultado es 0, devuélvelo como Nulo (Dato Faltante)
        # Esto maneja las entradas de '0' o '000'.
        if final_integer == 0:
            return None
            
        return final_integer
        
    except ValueError:
        # Esto debería ser raro después del re.sub, pero lo dejamos por seguridad.
        return None

def clean_to_timestamp(text: Union[str, float]) -> Union[pd.Timestamp, None]:
    """
    Convierte el texto de fecha/hora a un objeto Timestamp.
    """
    if pd.isna(text) or text == '':
        return None
    try:
        # pd.to_datetime es muy robusto para varios formatos
        return pd.to_datetime(text)
    except Exception:
        return None

def clean_to_boolean(text: Union[str, float]) -> Union[bool, None]:
    """
    Convierte respuestas afirmativas a True y negativas a False.
    """
    if pd.isna(text) or text == '':
        return None
    
    cleaned = str(text).lower().strip()
    
    # Evalúa respuestas comunes afirmativas
    return cleaned in ['si', 'sí', 'yes', 'true', 'contar'] 

def clean_string(text: Union[str, float]) -> str:
    """
    Limpieza básica de string: elimina espacios al inicio y final.
    """
    if pd.isna(text):
        return ''
    return str(text).strip()

def clean_certifications_to_array(text: str) -> List[str]:
    """
    Convierte un string de certificaciones separadas por delimitadores (;, o salto de línea)
    en una lista limpia de strings.
    """
    if pd.isna(text) or not str(text).strip():
        return [] # Retorna lista vacía para NULL/vacio en la base de datos (JSONB/ARRAY)
    
    text = str(text).strip()
    
    # 1. Reemplazar delimitadores comunes por uno estándar (el punto y coma es el más probable)
    # También manejará comas y posibles saltos de línea (\n)
    text = text.replace(';', '|').replace(',', '|').replace('\n', '|')
    
    # 2. Dividir, limpiar y filtrar
    clean_list = [
        item.strip().upper() # Estandarizar a MAYÚSCULAS para consistencia en la BD
        for item in text.split('|')
        if item.strip() # Asegurar que no se añadan strings vacíos ('')
    ]
    
    return clean_list

def clean_string_upper(text: Union[str, float]) -> str:
    """
    Limpieza de string y conversión a mayúsculas.
    """
    if pd.isna(text):
        return ''
    return str(text).upper().strip()

def clean_string_numeric(text: Union[str, float]) -> str:
    """
    Limpia texto dejando solo caracteres alfanuméricos y espacios.
    """
    if pd.isna(text):
        return ''
    # Elimina caracteres que no sean palabra, espacio o guion
    return re.sub(r'[^\w\s-]', '', str(text)).strip()

#

# Lista de acrónimos que SÍ deben ir en MAYÚSCULAS (Inicialismos)
ACRONYMS = [
    'CEO', 'CFO', 'CTO', 'COO', 'CIO', 'VP', 'HR', 'IT', 'MKT', 
    'PR', 'R&D', 'I+D', 'EHS', 'QC', 'QA', 'RH', 'RRHH' 
]
# Las abreviaturas como DIR, ADMON, GRAL se manejan por defecto con Title Case (se remueven de aquí)
ACRONYMS_LOWER = [a.lower() for a in ACRONYMS]

# Palabras de unión o preposiciones que deben ir en minúsculas (Stopwords)
STOPWORDS = ['y', 'de', 'del', 'la', 'los', 'las', 'con', 'para', 'por', 'un', 'una', 'el', 'a', 'en']

def clean_cargo_smart_case(text: Union[str, float]) -> str:
    """
    Normaliza el cargo usando Title Case, forzando preposiciones a minúsculas
    y acrónimos comunes a MAYÚSCULAS.
    """
    if pd.isna(text) or str(text).strip() == '':
        return ''
        
    # 1. Limpieza inicial: Reemplazar puntuación por espacios y normalizar
    cleaned = str(text).upper().strip()
    cleaned = re.sub(r'[^\w\s\-\+\&\.\/]', ' ', cleaned) # Mantener guiones/símbolos relevantes
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    # 2. Convertir a Title Case y separar en palabras
    words = cleaned.title().split()
    
    # 3. Aplicar lógica de minúsculas y mayúsculas
    final_words = []
    
    for i, word in enumerate(words):
        word_lower = word.lower()
        
        # A. Mantener la primera palabra siempre en mayúscula (Title Case)
        if i == 0 and word_lower in STOPWORDS:
            final_words.append(word)
        
        # B. Forzar a minúsculas para preposiciones/conjunciones (stopwords)
        elif word_lower in STOPWORDS:
            final_words.append(word_lower)
            
        # C. Forzar a mayúsculas para acrónimos (ej: HR, CEO, GRAL)
        elif word_lower in ACRONYMS_LOWER:
            # Opción 1: Simplemente pasar el lower_case a upper (más directo y robusto)
            final_words.append(word_lower.upper())
                
        # D. El resto de las palabras van en Title Case
        else:
            final_words.append(word)
    
    # 4. Unir las palabras y retornar
    return ' '.join(final_words).strip()

def rescue_names(row: pd.Series) -> pd.Series:
    """
    Aplica la lógica de corrección de nombres y apellidos, incluyendo:
    1. Eliminación de iniciales si coinciden con *cualquier* palabra del apellido.
    2. Eliminación de apellidos duplicados en el nombre.
    3. División de nombre/apellido si el campo de apellido está vacío.
    """
    nombre = str(row['nombre_limpio']).strip()
    apellido = str(row['apellido_limpio']).strip()
    
    if not nombre and not apellido:
        return row
    
    nombre_words = nombre.split()
    
    # -------------------------------------------------------------
    # 1. Mini Regla: Buscar y eliminar una INICIAL AISLADA en el NOMBRE
    # Caso: 'Horacio B', 'Valenzuela Bracamontes'
    # -------------------------------------------------------------
    if apellido and len(nombre_words) >= 2:
        
        possible_initial = nombre_words[-1] 
        
        # 1.1 Verificar si es una sola letra (o letra + punto)
        if len(possible_initial) <= 2 and possible_initial.isalpha():
            
            # 1.2 LÓGICA CLAVE: Comparar la inicial con TODAS las palabras del apellido
            apellido_words = apellido.split()
            initial_matched = False
            
            for a_word in apellido_words:
                if a_word.upper().startswith(possible_initial.upper()):
                    initial_matched = True
                    break

            if initial_matched:
                # La inicial coincide con el inicio de un apellido (ej. 'B' con 'Bracamontes').
                # La eliminamos del nombre.
                nombre_new = ' '.join(nombre_words[:-1]).strip()
                row['nombre_limpio'] = nombre_new
                nombre = nombre_new # Actualizar la variable para el paso 2

    # -------------------------------------------------------------
    # 2. Corrección: Si el apellido (o parte de él) se repite en el nombre
    # -------------------------------------------------------------
    if apellido and nombre:
        apellido_parts = apellido.split()
        
        # Intentar con el apellido completo
        if apellido in nombre:
            nombre_new = nombre.replace(apellido, '').strip()
            row['nombre_limpio'] = re.sub(r'\s+', ' ', nombre_new)
            
            if len(row['nombre_limpio'].split()) >= 1: return row
            row['nombre_limpio'] = nombre 

        # Intentar con el primer apellido
        elif len(apellido_parts) > 0 and apellido_parts[0] in nombre:
            nombre_new = nombre.replace(apellido_parts[0], '').strip()
            row['nombre_limpio'] = re.sub(r'\s+', ' ', nombre_new)
            return row

    # -------------------------------------------------------------
    # 3. Rescate: Si el apellido está vacío (División de palabras)
    # -------------------------------------------------------------
    if not apellido:
        words = nombre.split()
        num_words = len(words)
        
        if num_words >= 3:
            row['apellido_limpio'] = ' '.join(words[-2:])
            row['nombre_limpio'] = ' '.join(words[:-2])
            
        elif num_words == 2:
            row['apellido_limpio'] = words[-1]
            row['nombre_limpio'] = words[0]
            
    return row