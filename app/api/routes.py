from flask import Blueprint, render_template,  request
from ..services.google_sheets_service import save_user_data


main = Blueprint("main", __name__)

@main.route("/")
def home():
    return render_template("login.html")

@main.route("/registro", methods=["GET", "POST"])
def registro():
    if request.method == "POST":
        username = request.form.get("username")
        email = request.form.get("email")
        password = request.form.get("password")
        confirm_password = request.form.get("confirm_password")

        if password != confirm_password:
            error_message = "Las contraseñas no coinciden."
            return render_template("registro.html", error=error_message)
        
        user_data = [username, email, password]
    
        exito = save_user_data(
            credentials_path="C:/Users/angel/OneDrive/Escritorio/estadias/SEDECYT_ANALYTICS/credentials.json",
            sheet_id="1gk3y3YH2bX4t5v6w7x8y9z0A1B2C3D4E5F6G7H8I9J0K",
            worksheet_name="users",
            user_data=user_data
        )
    
        if exito:
            return render_template("login.html", message="Registro exitoso. Ahora puedes iniciar sesión.")
        
        else:
            error_message = "Error al registrar el usuario. Inténtalo de nuevo."
            return render_template("registro.html", error=error_message)

    return render_template("registro.html")
