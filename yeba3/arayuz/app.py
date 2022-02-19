#from crypt import methods

import os
import psycopg2
from flask import Flask, render_template, request, redirect, url_for
from os.path import exists

app = Flask(__name__)

database_name = "yhk_db"
user_name = "postgres"
password = "Tamam123"
host_ip = "127.0.0.1"
host_port = "5432"

try: 
    db = psycopg2.connect(database=database_name,
                      user=user_name,
                      password=password,
                      host=host_ip,
                      port=host_port)
except (Exception, psycopg2.Error) as error:
    print("Error while fetching data from PostgreSQL", error)

db.autocommit = True
cursor = db.cursor()

@app.route('/report')
def report():
    try:
        cursor.execute("""SELECT * from hava_kalitesi""")
        rows = cursor.fetchall()

        return render_template('report.html',  results=rows)
    except Exception as e:
        print(e)
        return []


@app.route("/")
def homepage():
    try:
        cursor.execute("""SELECT * FROM hava_kalitesi ORDER BY id DESC LIMIT 1""")
        rows = cursor.fetchall()

        return render_template('homepage.html',  results=rows)
    except Exception as e:
        print(e)
        return []


@app.route("/login", methods=["GET","POST"])
def login():
    cursor.execute("""SELECT * from user_login""")
    rows = cursor.fetchall()
    for row in rows:
        email=row[1]
        password=row[2]
    if request.method=="POST":
        print(request.form['email'])
        if email==request.form['email'] and password==request.form['password']:
            print("email dogru")
            return redirect(url_for('report'))
        else:
            return redirect(url_for('login'))
    
    return render_template('login.html',  results=rows)


if __name__ == "__main__":
    app.run(debug=True)
