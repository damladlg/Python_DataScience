#from crypt import methods

import os
import psycopg2
import flask_excel as excel
from flask import Flask, render_template, request, redirect, url_for, send_file,session
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
        start_total=request.form['start_date']+"T"+request.form['start_time']+":00"
        end_total=request.form['end_date']+"T"+request.form['end_time']+":00"
        print(start_total)
        print(end_total)
        cursor.execute("""SELECT * FROM hava_kalitesi WHERE ReadTime between %s and %s """, [start_total, end_total])
        rows = cursor.fetchall()

        if len(rows)!=0:
            excel.init_excel(app)
            extension_type = "csv"
            filename = "hava_kalitesi" + "." + extension_type

            d=[("id","ReadTime","Concentration PM10", "Concentration", "Concentration O3", "Concentration NO2", "Concentration CO",
            "AQI PM10", "AQI SO2", "AQI O3", "AQI NO2", "AQI CO", "AQI AQIIndex", "AQI Contaminant Parameter", "AQI Color")]

            for row in rows:
                d.append(row)
            return excel.make_response_from_array(d, file_type=extension_type, file_name=filename)
    return render_template('form.html')

class User:
    def __init__(self, id, email, password):
        self.id=id
        self.email=email
        self.password=password
    def __repr__(self):
        return f'<User: {self.email}>'

users=[]
app.secret_key='secretkeythatonly'
@app.route("/", methods=["GET","POST"])
def login():
    cursor.execute("""SELECT * from user_login""")
    rows = cursor.fetchall()
    for row in rows:
        #email=row[1]
        #password=row[2]
        users.append(User(id=row[0], email=row[1], password=row[2]))
    if request.method=="POST":
        session.pop('user_id', None)
        email=users[0].email
        password=users[0].password
        if email==request.form['email'] and password==request.form['password']:
            session['user_id']=users[0].id
            return redirect(url_for('index'))
        else:
             return redirect(url_for('login'))
    return render_template('login.html',  results=rows)

if __name__ == "__main__":
    app.run(debug=True)
