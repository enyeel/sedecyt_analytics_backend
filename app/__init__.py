from flask import Flask

def create_app():
    app = Flask(__name__)

    # importar y registrar rutas
    from .api.routes import main
    app.register_blueprint(main)

    return app
