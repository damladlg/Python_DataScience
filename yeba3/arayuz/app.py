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


@app.route("/index")
def index():
    try:
        cursor.execute("""SELECT * FROM hava_kalitesi ORDER BY id DESC LIMIT 1""")
        rows = cursor.fetchall()

        return render_template('index.html',  results=rows)
    except Exception as e:
        print(e)
        return []

@app.route("/form", methods=["GET","POST"])
def form():
    if request.method=="POST":
        print(request.form['start_date'])
        print(request.form['end_date'])
        print(request.form['start_time'])
        print(request.form['end_time'])
        start_total=request.form['start_date']+"T"+request.form['start_time']+":00"
        end_total=request.form['end_date']+"T"+request.form['end_time']+":00"
        print(start_total)
        print(end_total)

        cursor.execute("""SELECT * FROM hava_kalitesi WHERE ReadTime between %s and %s """, [start_total, end_total])
        rows = cursor.fetchall()
        print(rows)
        return redirect(url_for('index',results=rows))
    
    return render_template('form.html')


@app.route("/", methods=["GET","POST"])
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
            return redirect(url_for('index'))
        else:
            return redirect(url_for('login'))
    return render_template('login.html',  results=rows)

if __name__ == "__main__":
    app.run(debug=True)
