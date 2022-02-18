import os
import psycopg2
from flask import Flask, render_template
from os.path import exists

app = Flask(__name__)

database_name = "yhk_db"
user_name = "postgres"
password = "Tamam123"
host_ip = "127.0.0.1"
host_port = "5432"

db = psycopg2.connect(database=database_name,
                      user=user_name,
                      password=password,
                      host=host_ip,
                      port=host_port)

db.autocommit = True
cursor = db.cursor()


@app.route('/report')
def contacts():
    try:
        cursor.execute("""SELECT * from hava_kalitesi""")
        rows = cursor.fetchall()

        return render_template('report.html',  results=rows)
    except Exception as e:
        print(e)
        return []


@app.route("/")
def homepage():
    return render_template("homepage.html")


@app.route("/login")
def login():
    return render_template("login.html")


if __name__ == "__main__":
    app.run(debug=True)
