import re

def clean_rfc(text: str) -> str:
    """Normaliza RFCs y detecta IDs extranjeros."""
    raw = str(text).upper().strip()
    cleaned = re.sub(r'[^\w\s&Ñ]', '', raw).replace(" ", "")

    # 1. RFC Mexicano (Regex Estricto)
    if re.fullmatch(r'[A-Z&Ñ]{3,4}\d{6}[A-Z0-9]{3}', cleaned):
        return cleaned
    
    # 2. Tax ID Extranjero (Numérico)
    if cleaned.isdigit() and 8 <= len(cleaned) <= 15:
        return f'ID_EXT_{cleaned}'
        
    # 3. Fallo de Validación (Para revisión manual)
    return f'ID_FALLO_{cleaned[:15]}'



def clean_phone(text: str) -> str:
    """Convierte cualquier entrada a formato E.164 (+CountryCode)."""
    raw = str(text).strip()
    digits = re.sub(r'\D', '', raw) # Solo números
    
    # Caso 1: Internacional explícito (ej. +1 555...)
    if raw.startswith('+'):
        return f'+{digits}' if len(digits) >= 8 else ''

    # Caso 2: México (10 dígitos estándar)
    if len(digits) == 10:
        return f'+52{digits}'

    # Caso 3: México Antiguo (044, 045, 521...)
    if len(digits) >= 11 and digits.startswith('52'):
        # Elimina el '1' extra de celulares viejos
        clean_mx = digits[3:] if digits.startswith('521') else digits[2:]
        return f'+52{clean_mx}'

    return '' # Número inválido



def extract_certs(text: str, catalog: dict) -> list:
    """Extrae certificaciones ISO/NOM de texto narrativo."""
    text_upper = text.upper()
    found = set()

    # Ordenar keywords por longitud para priorizar "ISO 9001" sobre "9001"
    sorted_keys = sorted(catalog.keys(), key=len, reverse=True)

    for keyword in sorted_keys:
        # Búsqueda de palabra exacta (Word Boundaries)
        pattern = r'\b' + re.escape(keyword) + r'\b'
        
        if re.search(pattern, text_upper):
            acronym = catalog[keyword]
            found.add(acronym)
            # Eliminar hallazgo para evitar falsos positivos dobles
            text_upper = re.sub(pattern, '', text_upper)

    return sorted(list(found))