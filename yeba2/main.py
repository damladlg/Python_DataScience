import csv
import os
import statistics
import pandas as pd
import numpy as np
import datetime
from datetime import date, timedelta

filename = "yeba1_25.10.2021.csv"
df = pd.read_csv(filename, sep=',', names=['Id', 'Date-Time', 'Status', 'CO2', 'CO', 'O2', 'SO2', 'NO', 'T_Infra', 'T_PCB_Infra',
                 'T_Ultra', 'T_PCB_Ultra', 'FLOW', 'O2_raw', 'CO_raw', 'NO_raw', 'SO2_raw', 'O2_volt', 'CO_ref', 'NO_ref', 'SO2_ref'])
cust_list = df[1:]

count = 0
CO_toplam = 0
NO_toplam = 0
SO2_toplam = 0
O2_toplam = 0
eleman = 0
CO_avgs = []
NO_avgs = []
SO2_avgs = []
O2_avgs = []
sonuc = []
Date = []
baslangic_saatleri = []
bitis_saatleri = []
status_result = 0
for x in range(1, len(df)):
    dateTime = datetime.datetime.strptime(
        df['Date-Time'][x], '%Y-%m-%d %H:%M:%S')
    fileDate = dateTime.strftime('%d.%m.%Y')
    Date.append(fileDate)
    if df['Status'][x] == 'OK':
        if dateTime.minute < 30:
            CO_toplam = CO_toplam+float(df['CO'][x])
            NO_toplam = NO_toplam+float(df['NO'][x])
            SO2_toplam = SO2_toplam+float(df['SO2'][x])
            O2_toplam = O2_toplam+float(df['O2'][x])
            eleman = eleman+1
            if dateTime.minute == 0 and dateTime.second == 0:
                baslangic_saatleri.append(df['Date-Time'][x])
            if dateTime.minute == 29 and dateTime.second == 59:
                bitis_saatleri.append(df['Date-Time'][x])
                CO_avg = CO_toplam/eleman
                CO_avgs.append(CO_avg)
                NO_avg = NO_toplam/eleman
                NO_avgs.append(NO_avg)
                SO2_avg = SO2_toplam/eleman
                SO2_avgs.append(SO2_avg)
                O2_avg = (O2_toplam/eleman)/10000
                O2_avgs.append(O2_avg)
                count = count+1
                CO_toplam = 0
                NO_toplam = 0
                SO2_toplam = 0
                O2_toplam = 0
                status_result = eleman
                eleman = 0
                if status_result < 1201:
                    sonuc.append('Gecersiz Veri')
                else:
                    sonuc.append('Gecerli')
        if dateTime.minute >= 30 and dateTime.minute <= 59:
            CO_toplam = CO_toplam+float(df['CO'][x])
            NO_toplam = NO_toplam+float(df['NO'][x])
            SO2_toplam = SO2_toplam+float(df['SO2'][x])
            O2_toplam = O2_toplam+float(df['O2'][x])
            eleman = eleman+1
            if dateTime.minute == 30 and dateTime.second == 0:
                baslangic_saatleri.append(df['Date-Time'][x])
            if dateTime.minute == 59 and dateTime.second == 59:
                bitis_saatleri.append(df['Date-Time'][x])
                CO_avg = CO_toplam/eleman
                CO_avgs.append(CO_avg)
                NO_avg = NO_toplam/eleman
                NO_avgs.append(NO_avg)
                SO2_avg = SO2_toplam/eleman
                SO2_avgs.append(SO2_avg)
                O2_avg = (O2_toplam/eleman)/10000
                O2_avgs.append(O2_avg)
                count = count+1
                CO_toplam = 0
                NO_toplam = 0
                SO2_toplam = 0
                O2_toplam = 0
                status_result = eleman
                eleman = 0
                if status_result < 1201:
                    sonuc.append('Gecersiz Veri')
                else:
                    sonuc.append('Gecerli')


##### Verileri yeni bir csv dosyasına yazdırma #####

newFileName = "./yeba_data_analiz.csv"
file_exists = os.path.isfile(newFileName)
no = 0
if file_exists:
    # def sum1forline(filename):
    with open(newFileName) as f:
        count_line = sum(1 for line in f)
        no = count_line-1

# finalData = [[no, Date, sonuc, baslangic_saatleri, bitis_saatleri,
#              olcum_suresi, O2_avgs, CO_avgs, NO_avgs, SO2_avgs, O2_avgs]]
headers = ['Olcum No', 'Tarih', 'Sonuc', 'Olcum Baslangic Saati', 'Olcum Bitis Saati',
           'Toplam Olcum Suresi', 'Gecerli Olcum Suresi', 'CO', 'NO', 'SO2', 'O2']
with open(newFileName, 'a', encoding='UTF8', newline='') as file:
    fileWriter = csv.writer(file)
    if not file_exists:
        fileWriter.writerow(headers)
