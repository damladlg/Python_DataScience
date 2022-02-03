import csv
import os
import pandas as pd
import numpy as np

filename = "yeba1_14.11.2021.csv"

df = pd.read_csv(filename, sep=',', names=['Id', 'Date-Time', 'Status', 'CO2', 'CO', 'O2', 'SO2', 'NO', 'T_Infra', 'T_PCB_Infra',
                 'T_Ultra', 'T_PCB_Ultra', 'FLOW', 'O2_raw', 'CO_raw', 'NO_raw', 'SO2_raw', 'O2_volt', 'CO_ref', 'NO_ref', 'SO2_ref'])
cust_list = df[1:]

if len(cust_list) != 0:
    # def modification_date(filename):
    Tarih = cust_list.iloc[0, 1]
    VeriMiktari = len(cust_list.index) + 1
    tempIlkVeri = str(cust_list.iloc[0].values)
    IlkVeri = tempIlkVeri.split(',')
    tempSonVeri = str(cust_list.iloc[VeriMiktari-2].values)
    SonVeri = tempSonVeri.split(',')

########################################################################################################################

    intZEROValue = cust_list[cust_list['Status'].str.contains(pat='intZERO')]

    if (len(intZEROValue) != 0):
        intZEROIndex = cust_list.loc[cust_list['Status'] == 'intZERO'].index[0]
        ZeroDurumu = 'Evet'
        a = str(intZEROValue.values)
        zeroOncesiVeri = a.split(",")
        if str(-15) < intZEROValue.iloc[0, 3] > str(15) or str(-15) < intZEROValue.iloc[0, 5] > str(15) or str(-15) < intZEROValue.iloc[0, 6] > str(15):
            ZeroDurumu = "Uyari"
            print(intZEROValue.index)
        else:
            ZeroDurumu = "OK"

        tempzeroSonrasıVeri = str(cust_list.iloc[intZEROIndex+891].values)
        zeroSonrasıVeri = tempzeroSonrasıVeri.split(',')
    else:
        ZeroDurumu = 'Hayir'
        zeroOncesiVeri = "Normal"
        zeroSonrasıVeri = "Normal"

# hiç veri olmayan dosyalar icin
else:
    Tarih = " ",
    VeriMiktari = " "
    IlkVeri = " "
    SonVeri = " "
    ZeroDurumu = " "
    zeroOncesiVeri = " "
    zeroSonrasıVeri = " "

##### Verileri yeni bir csv dosyasına yazdırma #####

newFileName = "./yeba_data.csv"
file_exists = os.path.isfile(newFileName)
no = 0
if file_exists:
    # def sum1forline(filename):
    with open(newFileName) as f:
        count_line = sum(1 for line in f)
        no = count_line-1

finalData = [[no, Tarih[0:10], VeriMiktari, IlkVeri, SonVeri,
             ZeroDurumu, zeroOncesiVeri, zeroSonrasıVeri]]
headers = ['No', 'Tarih', 'VeriMiktari', 'IlkVeri', 'SonVeri',
           'ZeroDurumu', 'ZeroOncesiVeri', 'ZeroSonrasiVeri']

with open(newFileName, 'a', encoding='UTF8', newline='') as file:
    fileWriter = csv.writer(file)
    if not file_exists:
        fileWriter.writerow(headers)
    fileWriter.writerows(finalData)
