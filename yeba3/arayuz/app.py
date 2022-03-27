#from crypt import methods
import os
from random import choices
import psycopg2
import flask_excel as excel
from flask import Flask, render_template, request, redirect, url_for, send_file,session, g
from os.path import exists
from flask_wtf import FlaskForm
from wtforms.fields import DateField, TimeField, SelectField
from wtforms.validators import DataRequired
from wtforms import validators, SubmitField

app = Flask(__name__)
app.secret_key=os.urandom(24)

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

class InfoForm(FlaskForm):
    startdate = DateField('Başlangıç Tarihi', format='%Y-%m-%d', validators=(validators.DataRequired(),))
    enddate = DateField('Bitiş Tarihi', format='%Y-%m-%d', validators=(validators.DataRequired(),))
    starttime = TimeField('Başlangıç Saati', validators=(validators.DataRequired(),))
    endtime = TimeField('Bitiş Saati', validators=(validators.DataRequired(),))
    report = SelectField('Rapor Tipi', choices=[('1', 'Tüm Veriler'), ('2', 'Saatlik Ortalama Verileri'), ('3', 'Günlük Ortalama Verileri')])
    submit = SubmitField('Rapor Oluştur')

@app.before_request
def before_request():
    g.user=None
    if 'user' in session:
        g.user = session['user']

@app.route("/", methods=["GET","POST"])
def index():
    try:
        cursor.execute("""SELECT * FROM hava_kalitesi ORDER BY id DESC LIMIT 1""")
        rows = cursor.fetchall()
        if g.user:
            return render_template('index.html',  results=rows, user=session['user'])
        else:
            if request.method=="POST":
                return redirect(url_for('login'))
            else:
                return render_template('index.html',results=rows)
    except Exception as e:
        print(e)
        return []

@app.route("/form", methods=["GET","POST"])
def form():
    messages=" "
    form = InfoForm()
    if g.user:
        if form.validate_on_submit():
            start_total=str(form.startdate.data)+"T"+str(form.starttime.data)+"Z"
            end_total=str(form.enddate.data)+"T"+str(form.endtime.data)+"Z"
            cursor.execute("""SELECT * FROM hava_kalitesi WHERE ReadTime between %s and %s """, [start_total, end_total])
            rows = cursor.fetchall()
            if len(rows)!=0:
                messages="Rapor basarı ile çıktı."
                excel.init_excel(app)
                extension_type = "csv"
                filename = "hava_kalitesi" + "." + extension_type

                d=[("id","ReadTime","Concentration PM10", "Concentration", "Concentration O3", "Concentration NO2", "Concentration CO",
                "AQI PM10", "AQI SO2", "AQI O3", "AQI NO2", "AQI CO", "AQI AQIIndex", "AQI Contaminant Parameter", "AQI Color")]

                for row in rows:
                    d.append(row)
                return excel.make_response_from_array(d, file_type=extension_type, file_name=filename)
            else:
                messages="Geçerli tarih giriniz."
        return render_template('form.html', user=session['user'], form=form, messages=messages)
    return render_template('form.html', user=session['user'], form=form, messages=messages)

class User:
    def __init__(self, id, email, password):
        self.id=id
        self.email=email
        self.password=password
    def __repr__(self):
        return f'<User: {self.email}>'

users=[]

@app.route("/login", methods=["GET","POST"])
def login():
    cursor.execute("""SELECT * from user_login""")
    rows = cursor.fetchall()
    print(rows)
    for row in rows:
        #email=row[1]
        #password=row[2]
        users.append(User(id=row[0], email=row[1], password=row[2]))
    print(users)
    if request.method=="POST":
        session.pop('user', None)
        email=users[0].email
        password=users[0].password
        if email==request.form['email'] and password==request.form['password']:
            session['user']=request.form['email']
            return redirect(url_for('index'))
        else:
            return redirect(url_for('login'))
    return render_template('login.html',  results=rows)

@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect('/login')

if __name__ == "__main__":
    app.run(debug=True)
