import requests
from urllib.parse import unquote
import json
import psycopg2
from datetime import datetime, timedelta
import schedule
import time
import pandas as pd
from requests.exceptions import HTTPError
import psycopg2.extras as extras

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

def query_to_df(rows):
    df = pd.DataFrame(rows, columns=["pm2_5_ugpm3_avg", "PM4_ugpm3_avg", "PM10_ugpm3_avg", "Pressure_avg", "Temperature_avg", "Humidity_avg", "NO2_avg", "O3_avg", "time"])
    df['time'] = pd.to_datetime(df['time'], errors='coerce')
    df['time'] = df['time'].dt.strftime('%m-%d-%Y %H:%M:%S')
    df.index = pd.to_datetime(df['time'])

    # Range disinda olan verileri at PM
    print("PM2.5 1000 uzeri olanlar:")
    print(len(df[df['pm2_5_ugpm3_avg'] > 1000]))
                
    df = df[df['pm2_5_ugpm3_avg'] < 1000] 

    print("PM2.5 sıfırın altında olanlar:")
    print(len(df[df['pm2_5_ugpm3_avg'] < 0]))
    df = df[df['pm2_5_ugpm3_avg'] > 0]

    # Range disinda olan verileri at O3
    print("O3 200 uzeri olanlar:")
    print(len(df[df['O3_avg'] > 200]))
    df = df[df['O3_avg'] < 200]

    print("O3 sıfırın altında olanlar:")
    print(len(df[df['O3_avg'] < 0]))
    df = df[df['O3_avg'] > 0]

    # Range disinda olan verileri at NO2
    print("NO2 200 uzeri olanlar:")
    print(len(df[df['NO2_avg'] > 200]))
    df = df[df['NO2_avg'] < 200]

    print("NO2 sıfırın altında olanlar:")
    print(len(df[df['NO2_avg'] < 0]))
    df = df[df['NO2_avg'] > 0]
    
    return df

def execute_values(conn, df, table):
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
  
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("Veri tabanına eklendi.")
    cursor.close()

def job_hourly_avg():
    now=datetime.now()
    now_date_string = now.strftime("%Y-%m-%d")
    now_time_string = now.strftime("%H:%M:%S")
    now=now_date_string+"T"+now_time_string+"Z"

    one_hour_ago=datetime.now() - timedelta(hours = 1)
    one_hour_ago_date_string = one_hour_ago.strftime("%Y-%m-%d")
    one_hour_ago_time_string = one_hour_ago.strftime("%H:%M:%S")

    if now_time_string=="00:00:00": #00:00 da 1 saat öncesini alınca day değeri bir önceki gün olması gerektiği için, yoksa yeni günün day ını alıyor
        one_hour_ago_date_string=(datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%d")
        one_hour_ago=one_hour_ago_date_string+"T"+one_hour_ago_time_string+"Z"
    else:
        one_hour_ago=one_hour_ago_date_string+"T"+one_hour_ago_time_string+"Z"
                
    #saatlik ortalama bulmak için sorgu ile 1 saatlik aralık geliyor 
    cursor.execute("""SELECT pm2_5_ugpm3, PM4_ugpm3, PM10_ugpm3, Pressure, Temperature, Humidity, NO2, O3, time FROM hava_kalitesi WHERE time between %s and %s""", [one_hour_ago, now])
    query_hourly_rows = cursor.fetchall()
    
    dataframe=query_to_df(query_hourly_rows)
    
    df_hourly_avg = dataframe.resample('H').mean()
    df_hourly_avg['Date_time'] = df_hourly_avg.index
    df_hourly_avg=df_hourly_avg.head(1)
    
    execute_values(db, df_hourly_avg, "hava_kalitesi_saatlik_ortalama")
    
    print(df_hourly_avg)

def job_daily_avg():
    now = datetime.now().strftime("%Y-%m-%d")+"T"+datetime.now().strftime("%H:%M:%S")+"Z"
    one_day_ago=datetime.now() - timedelta(days = 1)
    one_day_ago = one_day_ago.strftime("%Y-%m-%d")+"T"+one_day_ago.strftime("%H:%M:%S")+"Z"
                
    cursor.execute("""SELECT pm2_5_ugpm3, PM4_ugpm3, PM10_ugpm3, Pressure, Temperature, Humidity, NO2, O3, time FROM hava_kalitesi WHERE time between %s and %s""", [one_day_ago, now])
    query_daily_rows = cursor.fetchall()
    
    dataframe=query_to_df(query_daily_rows)
    
    df_daily_avg = dataframe.resample('D').mean()
    df_daily_avg['Date_time'] = df_daily_avg.index
    df_daily_avg=df_daily_avg.head(1)
    
    execute_values(db, df_daily_avg, "hava_kalitesi_gunluk_ortalama")
    
    print(df_daily_avg)

def job_seconds_data():
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

schedule.every().seconds.do(job_seconds_data)
schedule.every().hours.do(job_hourly_avg)
schedule.every().day.at("00:00").do(job_daily_avg)

while True:
    schedule.run_pending()
    time.sleep(1)