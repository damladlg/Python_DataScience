import requests
from urllib.parse import unquote
import json
import psycopg2
from datetime import datetime, timedelta
import schedule
import time
from requests.exceptions import HTTPError

def request_to_url(start_date,end_date):
    parameters = {
        "StationId": "377e1216-bcc7-42c0-aad8-4d5b3d602b78",
        "StartDate": start_date,
        "EndDate": end_date
        }
    payload = {}
    headers= {}
    url="https://api.ibb.gov.tr/havakalitesi/OpenDataPortalHandler/GetAQIByStationId"
    
    try:
        istek = requests.get(url, params = parameters)
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
        
        
start_date="01.02.2022%2000:00:00"
end_date="02.02.2022%2000:00:00"      
insert_data=request_to_url(start_date,end_date)
print(insert_data)
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
database_name = "yhk_db"
if (database_name,) in list_database:
    print("'{}' veri tabanı zaten mevcut. Mevcut olan kullaniliyor.".format(database_name))
else:
    query_create_database = "CREATE DATABASE yhk_db"
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

total=0.0
try:
    for i in insert_data:
        if i['AQI'] is not None:
            print(i['ReadTime'])
            print(i['Concentration']['PM10'])
            if i['Concentration']['PM10'] is None:
                i['Concentration']['PM10']=0.0
            total=total+i['Concentration']['PM10']
            print(total)
            if "59:59" in i['ReadTime']: 
                print("yes")
                total=0.0
                
            
            query_insert_table="""INSERT into hava_kalitesi(ReadTime, Concentration_PM10, Concentration_SO2, Concentration_O3, Concentration_NO2, Concentration_CO, AQI_PM10, AQI_SO2, AQI_O3, AQI_NO2, AQI_CO, AQI_AQIIndex, AQI_ContaminantParameter, AQI_State, AQI_Color) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            record_to_insert = (i['ReadTime'], i['Concentration']['PM10'], i['Concentration']['SO2'],i['Concentration']['O3'],
                                i['Concentration']['NO2'],i['Concentration']['CO'],i['AQI']['PM10'],i['AQI']['SO2'],
                                i['AQI']['O3'], i['AQI']['NO2'],i['AQI']['CO'], i['AQI']['AQIIndex'], i['AQI']['ContaminantParameter'],
                                i['AQI']['State'],i['AQI']['Color'])
            cursor.execute(query_insert_table, record_to_insert)
except Exception as e:
    print ("TOPLU VERI AKTARIMI SIRASINDA HATA: " + str(e))

def job():
    try:
        record_to_insert_hourly = tuple()
        now = datetime.now()
        one_hour_ago=datetime.now() - timedelta(hours = 1)
        date_string = now.strftime("%d.%m.%Y")
        time_string = now.strftime("%H:00:00")
        time_string_one_hour_ago=one_hour_ago.strftime("%H:00:00")
        if time_string=="00:00:00": #00:00 da 1 saat öncesini alınca day değeri bir önceki gün olması gerektiği için, yoksa yeni günün day ını alıyor
            one_day_ago=(datetime.now() - timedelta(days = 1)).strftime("%d.%m.%Y")
            start_date=one_day_ago+"%20"+time_string_one_hour_ago
        else:
            start_date=date_string+"%20"+time_string_one_hour_ago
        end_date=date_string+"%20"+time_string
        
        hourly_result=request_to_url(start_date,end_date)
        
        query_select_lastrow="""SELECT ReadTime FROM hava_kalitesi
        ORDER BY id DESC
        LIMIT 1"""
        cursor.execute(query_select_lastrow)
        last_row=cursor.fetchone()
        current_hour="('"+hourly_result[0]['ReadTime']+"',)" 
        last_row=str(last_row)
        if last_row==current_hour:
            print("Bu veri eklenmis.")
        else:
            query_insert_table_hourly="""INSERT into hava_kalitesi(ReadTime, Concentration_PM10, Concentration_SO2, Concentration_O3, Concentration_NO2, Concentration_CO, AQI_PM10, AQI_SO2, AQI_O3, AQI_NO2, AQI_CO, AQI_AQIIndex, AQI_ContaminantParameter, AQI_State, AQI_Color) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            record_to_insert_hourly = (hourly_result[0]['ReadTime'], hourly_result[0]['Concentration']['PM10'], hourly_result[0]['Concentration']['SO2'],hourly_result[0]['Concentration']['O3'],
                                hourly_result[0]['Concentration']['NO2'],hourly_result[0]['Concentration']['CO'],hourly_result[0]['AQI']['PM10'],hourly_result[0]['AQI']['SO2'],
                                hourly_result[0]['AQI']['O3'], hourly_result[0]['AQI']['NO2'],hourly_result[0]['AQI']['CO'], hourly_result[0]['AQI']['AQIIndex'], hourly_result[0]['AQI']['ContaminantParameter'],
                                hourly_result[0]['AQI']['State'],hourly_result[0]['AQI']['Color'])
            cursor.execute(query_insert_table_hourly, record_to_insert_hourly)
            print("Son veri veri tabanina yazildi.")
            
        hourly_result.clear()
    except Exception as e:
        print ("SORGU SIRASINDA HATA: " + str(e))    

schedule.every().hour.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)