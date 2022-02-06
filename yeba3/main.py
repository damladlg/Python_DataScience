import requests
from urllib.parse import unquote
import json
import psycopg2
from datetime import datetime
import schedule
import time

start_date="06.02.2022%2014:00:00"
end_date="06.02.2022%2015:00:00"  

parameters = {
"StationId": "377e1216-bcc7-42c0-aad8-4d5b3d602b78",
"StartDate": start_date,
"EndDate": end_date
}
payload = {}
headers= {}
url="https://api.ibb.gov.tr/havakalitesi/OpenDataPortalHandler/GetAQIByStationId"

istek = requests.get(url, params = parameters)
if istek.status_code == 200:
    decodeUrl = unquote(istek.url) #invalid degerlerden dolayi url decode ediliyor.

    response = requests.get(decodeUrl,headers=headers, data = payload)
    result = json.loads(response.text)
    print("İstek basariyla atildi. Json veriler cekildi.")
else:
    print("İstek karsilanamadi.")
    
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
database_name = "yeba_db"
if (database_name,) in list_database:
    print("'{}' veri tabanı zaten mevcut. Mevcut olan kullaniliyor.".format(database_name))
else:
    query_create_database = "CREATE DATABASE yeba_db"
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
    ReadTime TEXT NOT NULL,
    Concentration_PM10 FLOAT,
    Concentration_SO2 FLOAT,
    Concentration_O3 FLOAT,
    Concentration_NO2 FLOAT,
    Concentration_CO FLOAT,
    AQI_PM10 FLOAT,
    AQI_SO2 FLOAT,
    AQI_O3 FLOAT,
    AQI_NO2 FLOAT,
    AQI_CO FLOAT,
    AQI_AQIIndex FLOAT,
    AQI_ContaminantParameter TEXT,
    AQI_State TEXT,
    AQI_Color TEXT
    )

"""
db.autocommit=True
cursor=db.cursor()
cursor.execute(query_create_table)

for i in result:
    query_insert_table="""INSERT into hava_kalitesi(ReadTime, Concentration_PM10, Concentration_SO2, Concentration_O3, Concentration_NO2, Concentration_CO, AQI_PM10, AQI_SO2, AQI_O3, AQI_NO2, AQI_CO, AQI_AQIIndex, AQI_ContaminantParameter, AQI_State, AQI_Color) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    record_to_insert = (i['ReadTime'], i['Concentration']['PM10'], i['Concentration']['SO2'],i['Concentration']['O3'],
                        i['Concentration']['NO2'],i['Concentration']['CO'],i['AQI']['PM10'],i['AQI']['SO2'],
                        i['AQI']['O3'], i['AQI']['NO2'],i['AQI']['CO'], i['AQI']['AQIIndex'], i['AQI']['ContaminantParameter'],
                        i['AQI']['State'],i['AQI']['Color'])
    cursor.execute(query_insert_table, record_to_insert)

def job():
    now = datetime.now()
    date_string = now.strftime("%d.%m.%Y")
    time_string = now.strftime("%H:00:00")
    start_date=date_string+"%20"+time_string
    end_date=date_string+"%20"+time_string
    parameters = {
    "StationId": "377e1216-bcc7-42c0-aad8-4d5b3d602b78",
    "StartDate": start_date,
    "EndDate": end_date
    }
    payload = {}
    headers= {}
    url="https://api.ibb.gov.tr/havakalitesi/OpenDataPortalHandler/GetAQIByStationId"

    istek = requests.get(url, params = parameters)
    if istek.status_code == 200:
        decodeUrl = unquote(istek.url) #invalid degerlerden dolayi url decode ediliyor.

        response = requests.get(decodeUrl,headers=headers, data = payload)
        hourly_result = json.loads(response.text)
        for i in hourly_result:
            query_insert_table_hourly="""INSERT into hava_kalitesi(ReadTime, Concentration_PM10, Concentration_SO2, Concentration_O3, Concentration_NO2, Concentration_CO, AQI_PM10, AQI_SO2, AQI_O3, AQI_NO2, AQI_CO, AQI_AQIIndex, AQI_ContaminantParameter, AQI_State, AQI_Color) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            record_to_insert_hourly = (i['ReadTime'], i['Concentration']['PM10'], i['Concentration']['SO2'],i['Concentration']['O3'],
                                i['Concentration']['NO2'],i['Concentration']['CO'],i['AQI']['PM10'],i['AQI']['SO2'],
                                i['AQI']['O3'], i['AQI']['NO2'],i['AQI']['CO'], i['AQI']['AQIIndex'], i['AQI']['ContaminantParameter'],
                                i['AQI']['State'],i['AQI']['Color'])
            cursor.execute(query_insert_table_hourly, record_to_insert_hourly)
            print("Tabloya yazdı.")
        print("Saatlik istek atildi.")
        hourly_result.clear()
    else:
        print("İstek karsilanamadi.")

schedule.every().hour.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)