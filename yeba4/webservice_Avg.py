import psycopg2
import pandas as pd
import psycopg2.extras as extras

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

cursor.execute("""SELECT pm2_5_ugpm3, PM4_ugpm3, PM10_ugpm3, Pressure, Temperature, Humidity, NO2, O3, time FROM hava_kalitesi""")
rows = cursor.fetchall()

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

df_hourly_avg = df.resample('H').mean()  
df_hourly_avg['Date_time'] = df_hourly_avg.index
df_daily_avg = df.resample('D').mean()
df_daily_avg['Date_time'] = df_daily_avg.index

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

execute_values(db, df_daily_avg, table1)
execute_values(db, df_hourly_avg, table2)







    
