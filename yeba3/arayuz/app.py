from distutils.log import debug
from flask import Flask, render_template

app = Flask(__name__)


@app.route("/")
def login():
    return render_template("login.html")


@app.route("/search")
def search():
    return "Search"


if __name__ == "__main__":
    app.run(debug=True)
