import psycopg2
import pandas as pd
import psycopg2.extras as extras
from datetime import datetime, timedelta

database_name="web_service_db"
user_name="postgres"
password="Tamam123"
host_ip="127.0.0.1"
host_port="5432"

db=psycopg2.connect(database=database_name,
                    user=user_name,
                    password=password,
                    host=host_ip,
                    port=host_port)
cursor = db.cursor()

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

dataframe=query_to_df(query_hourly_rows)

df_hourly_avg = dataframe.resample('H').mean()
df_hourly_avg['Date_time'] = df_hourly_avg.index
df_hourly_avg=df_hourly_avg.head(1)

# df_daily_avg = dataframe.resample('D').mean()
# df_daily_avg['Date_time'] = df_daily_avg.index
# df_daily_avg=df_daily_avg.head(1)

print(df_hourly_avg)
table1="hava_kalitesi_gunluk_ortalama"
table2="hava_kalitesi_saatlik_ortalama" 

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

#execute_values(db, df_daily_avg, table1)
execute_values(db, df_hourly_avg, table2)







    
