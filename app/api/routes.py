from flask import render_template, request, redirect, url_for, flash, Blueprint
from app.services.google_sheets_service import save_user_data
from werkzeug.security import generate_password_hash


main = Blueprint("main", __name__)

@main.route("/")
def home():
    return render_template("login.html")

#@main.route("/registro", methods=["GET", "POST"])
#def registro():
#    if request.method == "POST":
#        username = request.form.get("username")
#        email = request.form.get("email")
#        password = request.form.get("password")
#        confirm_password = request.form.get("confirm_password")
#
#        if password != confirm_password:
#            error_message = "Las contraseñas no coinciden."
#            return render_template("registro.html", error=error_message)
#        
#        user_data = [username, email, password]
#
#        exito = save_user_data(
#            credentials_path="C:/Users/angel/OneDrive/Escritorio/estadias/SEDECYT_ANALYTICS/credentials.json",
#            sheet_id="1gk3y3YH2bX4t5v6w7x8y9z0A1B2C3D4E5F6G7H8I9J0K",
#            worksheet_name="users",
#            user_data=user_data
#            )
#
#        if exito:
#            return render_template("login.html", message="Registro exitoso. Ahora puedes iniciar sesión.")
#    
#        else:
#            error_message = "Error al registrar el usuario. Inténtalo de nuevo."
#            return render_template("registro.html", error=error_message)
#
#    return render_template("registro.html")

@main.route("/registro", methods=["GET", "POST"])
def registro():
    # El código solo se ejecuta cuando el usuario envía el formulario (POST)
    if request.method == "POST":
        # --- TODA LA LÓGICA DEBE ESTAR INDENTADA AQUÍ DENTRO ---
        
        username = request.form.get("username")
        email = request.form.get("email")
        password = request.form.get("password")
        confirm_password = request.form.get("confirm_password")

        # 1. Validar que las contraseñas coincidan
        if password != confirm_password:
            flash("Las contraseñas no coinciden.", "danger")
            return redirect(url_for('main.registro')) # Redirige de vuelta al formulario
        
        # 2. Hashear la contraseña para guardarla de forma segura
        password_hasheada = generate_password_hash(password)
        
        # 3. Preparar la fila para Google Sheets con la contraseña hasheada
        user_data = [username, email, password_hasheada]

        # 4. Llamar al servicio para guardar la fila
        exito = save_user_data(
            credentials_path="ruta/a/tus/credentials.json", # Es mejor usar una config
            sheet_id="tu-sheet-id-aqui",
            worksheet_name="users",
            user_data=user_data
            )

        # 5. Redirigir según el resultado
        if exito:
            flash("¡Registro exitoso! Ahora puedes iniciar sesión.", "success")
            return redirect(url_for('main.home')) # Redirige al login
        else:
            flash("Error al registrar el usuario. Inténtalo de nuevo.", "danger")
            return redirect(url_for('main.registro'))

    # Si el método es GET, simplemente muestra la página de registro
    return render_template("registro.html") 