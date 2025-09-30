from flask import Blueprint, render_template

main = Blueprint("main", __name__)

@main.route("/")
def home():
    return render_template("login.html")

@main.route("/ping")
def ping():
    return jsonify({"status": "ok"})
