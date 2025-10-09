import re

def phone_cleaning(telefonos: list) -> list:
    """
    Limpia y estandariza números de teléfono, priorizando el formato +52 (México).
    Los números con otros códigos de país (+) se limpian y se mantienen.
    """
    cleaned_phones = []

    for num in telefonos:
        if num is None:
            cleaned_phones.append("N/A")
            continue

        num = str(num).strip()

        # 1. Limpieza inicial: Manejar el '+' y eliminar otros caracteres no numéricos
        if num.startswith('+'):
            # Mantener el '+' y limpiar el resto
            num_digits_only = re.sub(r'[^0-9]', '', num[1:])
            num_clean = '+' + num_digits_only
        else:
            # Limpiar todos los caracteres no numéricos
            num_digits_only = re.sub(r'[^0-9]', '', num)
            num_clean = num_digits_only

        numero_final = num_clean # Valor por defecto

        # --- LOGICA DE PRIORIDAD ---

        # Prioridad 1: Números con CÓDIGO DE PAÍS que NO es +52 (ej. +351)
        if num_clean.startswith('+') and not num_clean.startswith('+52'):
            # Se asume que el usuario lo puso correctamente. Solo limpiar.
            numero_final = num_clean

        # Prioridad 2: Números con +52 (México)
        elif num_clean.startswith('+52'):
            # num_digits_only ya está limpio (solo dígitos, sin el '+')
            if len(num_digits_only) == 12 and num_digits_only.startswith('521'):
                # Caso +521449... (12 dígitos). Quitar el '1' extra de móvil.
                # Queda +52 + 10 dígitos
                numero_final = '+52' + num_digits_only[3:]
            elif len(num_digits_only) == 12 and num_digits_only.startswith('52'):
                # Caso +52449... (10 dígitos después del 52). Se mantiene.
                numero_final = '+52' + num_digits_only[2:]
            elif len(num_digits_only) == 11 and num_digits_only.startswith('52'):
                # Caso +52 y 9 dígitos, puede ser error o formato viejo. Se mantiene.
                numero_final = '+52' + num_digits_only[2:]
            else:
                # Otros casos con +52.
                numero_final = num_clean

        # Prioridad 3: Números mexicanos de 10 dígitos sin prefijo (LADA)
        elif len(num_clean) == 10:
            # 3512445730, 4491234567, etc.
            # Se ASUME que es México y se añade el +52
            numero_final = '+52' + num_clean

        # Prioridad 4: Números que empiezan con 52 (sin el '+')
        elif len(num_clean) >= 11 and num_clean.startswith('52'):
            # Caso 521449... (12 dígitos). Quitar el '1' y añadir el '+'
            if num_clean.startswith('521') and len(num_clean) == 12:
                numero_final = '+52' + num_clean[3:]
            else:
                # Caso 52449... (12 dígitos). Añadir '+' y dejar los 10 dígitos.
                numero_final = '+52' + num_clean[2:]

        # Prioridad 5: Casos restantes (incompletos, etc.)
        else:
            # Si el número resultante es muy corto, marcar como error
            if len(num_clean) < 7:
                numero_final = "ERROR: " + num

        cleaned_phones.append(numero_final)

    return cleaned_phones

# --- EJEMPLO DE USO ---
ex_data = [
    "5214491871188",     # 521 + 10 digitos -> +524491871188
    "4491075551",        # 10 digitos -> +524491075551
    "+52 449 122 73",    # +52, pero incompleto. Lógica lo limpia a +5244912273
    "+351 244 572 227",  # Portugal -> +351244572227
    "449-9222100",       # Con guion, 10 digitos -> +524499222100
    "3512445730",        # Ambigüedad (10 digitos) -> +523512445730 (Asume México)
    "912345678",         # 9 digitos, no es 10 -> ERROR
]

cleaned_phones_example = phone_cleaning(ex_data)

for original, limpio in zip(ex_data, cleaned_phones_example):
    print(f"Original: {original:<20} -> Limpio: {limpio}")