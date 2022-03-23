from flask import jsonify
from flask import Flask
import threading
import time

app = Flask(__name__)


data ={
  "BME280stat": "OK", 
  "ECstat": "OK", 
  "SPS30stat": "OK", 
  "SerialNo": "01", 
  "caldata": {
    "stat": ""
  }, 
  "dstat": "", 
  "errorcount": 0, 
  "meas": {
    "adu": {
      "AEu_NO2_257": 238.5, 
      "AEu_O3_557": 226.4, 
      "WEu_NO2_257": 240.2, 
      "WEu_O3_557": 234.1
    }, 
    "phy": {
      "Humidity": 1.0, 
      "NO2": 2.0, 
      "O3": 3.0, 
      "PM0_5_ppcm3": 4.0, 
      "PM10_ppcm3": 5.0, 
      "PM10_ugpm3": 6.0,  
      "PM1_ppcm3": 7.0, 
      "PM1_ugpm3": 8.0,  
      "PM2_5_ppcm3": 9.0, 
      "PM2_5_ugpm3": 10.0, 
      "PM4_ppcm3": 11.0, 
      "PM4_ugpm3": 12.0,  
      "Pressure": 13.0, 
      "Temperature": 14.0, 
      "typPartSize_um": 15.0, 
    }, 
    "ref": {}
  }, 
  "stat": "OK", 
  "time": "2022-02-24T11:17:54Z"
}

data0 = data

def json_time():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ')

def dataVirtual():
    global data
    while True:
        for param in data["meas"]["phy"]:
            curdata = data["meas"]["phy"][param]
            if curdata >= 59:
                data["meas"]["phy"][param] = curdata - 59
            else:
                data["meas"]["phy"][param] = curdata + 1
        data["time"] = json_time()

        if data["meas"]["phy"]["typPartSize_um"] > 20:
           data = data0
            
        time.sleep(1)


child_thread = threading.Thread(target=dataVirtual)
child_thread.start()

@app.route('/data')
def hello():
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='localhost', port=5000)