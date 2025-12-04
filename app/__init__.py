from flask import Flask
from flask_cors import CORS
import os

def create_app():
    app = Flask(__name__)
    
    cors_origin = os.getenv('FRONTEND_URL')
    CORS(app, resources={r"/api/*": {"origins": cors_origin}}, supports_credentials=True) 
    
    # importar y registrar rutas
    from .api.routes import api_bp
    app.register_blueprint(api_bp, url_prefix="/api")

    return app
