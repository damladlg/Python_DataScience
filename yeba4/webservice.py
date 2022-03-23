import requests
from urllib.parse import unquote
import json
import psycopg2
from datetime import datetime, timedelta
import schedule
import time
from requests.exceptions import HTTPError

def request_to_url():
    payload = {}
    headers= {}
    url="http://localhost:5000/data"
    
    try:
        istek = requests.get(url)
        if istek.status_code == 200:
            decodeUrl = unquote(istek.url) #invalid degerlerden dolayi url decode ediliyor.
            response = requests.get(decodeUrl,headers=headers, data = payload)
            result = json.loads(response.text) 
            print("İstek basariyla atildi. Json veriler cekildi.")
            return result
        else:
            print("İstek karsilanamadi.")

    except HTTPError as http_err:
        print('HTTP error occurred: {http_err}')
    except Exception as err:
        print('Other error occurred: {err}')
        

database_name="postgres"
user_name="postgres"
password="Tamam123"
host_ip="127.0.0.1"
host_port="5432"

db=psycopg2.connect(database=database_name,
                    user=user_name,
                    password=password,
                    host=host_ip,
                    port=host_port)

db.autocommit=True
cursor=db.cursor()

cursor.execute("SELECT datname FROM pg_database;")
list_database = cursor.fetchall()
database_name = "web_service_db"
if (database_name,) in list_database:
    print("'{}' veri tabanı zaten mevcut. Mevcut olan kullaniliyor.".format(database_name))
else:
    query_create_database = "CREATE DATABASE web_service_db"
    cursor.execute(query_create_database)
    print("Veri tabani olusturuldu.")
    
db=psycopg2.connect(database=database_name,
                    user=user_name,
                    password=password,
                    host=host_ip,
                    port=host_port)

query_create_table="""
CREATE TABLE IF NOT EXISTS hava_kalitesi(
    id SERIAL PRIMARY KEY,
    BME280stat TEXT,
    ECstat TEXT,
    SPS30stat TEXT,
    SerialNo INT,
    caldata_stat TEXT,
    dstat TEXT,
    errorcount INT, 
    AEu_NO2_257 FLOAT,
    AEu_O3_557 FLOAT,
    WEu_NO2_257 FLOAT,
    WEu_O3_557 FLOAT,
    Humidity FLOAT,
    NO2 FLOAT,
    O3 FLOAT,
    PM0_5_ppcm3 FLOAT,
    PM10_ppcm3 FLOAT,
    PM10_ugpm3 FLOAT,
    PM1_ppcm3 FLOAT,
    PM1_ugpm3 FLOAT,
    PM2_5_ppcm3 FLOAT,
    PM2_5_ugpm3 FLOAT,
    PM4_ppcm3 FLOAT,
    PM4_ugpm3 FLOAT,
    Pressure FLOAT,
    Temperature FLOAT,
    typPartSize_um FLOAT,
    stat TEXT,
    time TEXT
    )

"""
db.autocommit=True
cursor=db.cursor()
cursor.execute(query_create_table)

def job():
    try:
        insert_data=request_to_url()
        try:
            for i in range(len(insert_data)):
                query_insert_table="""INSERT into hava_kalitesi(BME280stat, ECstat, SPS30stat, SerialNo, caldata_stat, dstat, errorcount, AEu_NO2_257, AEu_O3_557, WEu_NO2_257, WEu_O3_557, Humidity, NO2, O3, PM0_5_ppcm3, PM10_ppcm3, PM10_ugpm3, PM1_ppcm3, PM1_ugpm3, PM2_5_ppcm3, PM2_5_ugpm3, PM4_ppcm3, PM4_ugpm3, Pressure, Temperature, typPartSize_um, stat, time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                record_to_insert = (insert_data['BME280stat'], insert_data['ECstat'], insert_data['SPS30stat'],insert_data['SerialNo'], insert_data['caldata']['stat'],
                                    insert_data['dstat'],insert_data['errorcount'],insert_data['meas']['adu']['AEu_NO2_257'],insert_data['meas']['adu']['AEu_O3_557'], insert_data['meas']['adu']['WEu_NO2_257'], insert_data['meas']['adu']['WEu_O3_557'],
                                    insert_data['meas']['phy']['Humidity'],insert_data['meas']['phy']['NO2'], insert_data['meas']['phy']['O3'], insert_data['meas']['phy']['PM0_5_ppcm3'], insert_data['meas']['phy']['PM10_ppcm3'],
                                    insert_data['meas']['phy']['PM10_ugpm3'],insert_data['meas']['phy']['PM1_ppcm3'], insert_data['meas']['phy']['PM1_ugpm3'], insert_data['meas']['phy']['PM2_5_ppcm3'], insert_data['meas']['phy']['PM2_5_ugpm3'],
                                    insert_data['meas']['phy']['PM4_ppcm3'],insert_data['meas']['phy']['PM4_ugpm3'], insert_data['meas']['phy']['Pressure'], insert_data['meas']['phy']['Temperature'], insert_data['meas']['phy']['typPartSize_um'],
                                    insert_data['stat'],insert_data['time'])
                if i==len(insert_data)-1:
                    cursor.execute(query_insert_table, record_to_insert)
                
        except Exception as e:
            print ("TOPLU VERI AKTARIMI SIRASINDA HATA: " + str(e))
    
        insert_data.clear()
    except Exception as e:
        print ("SORGU SIRASINDA HATA: " + str(e))    

schedule.every().seconds.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)