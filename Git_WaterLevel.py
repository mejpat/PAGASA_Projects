import json
import requests
from datetime import date
from datetime import timedelta
import csv
import os

tabList_URL = "http://121.58.193.173:8080/water/table_list.do"
#data needed
#ymdhm : "202106092350"
#the date/time format is YYYYMMDDHHMM

detList_URL = "http://121.58.193.173:8080/water/detail_list.do"
#data needed
#obscd : "11105201"
#ymdhm : "202106092120"

#NOTES:
# obscd is paired with the number related with the station name (obsnm) e.g. obscd 11105201 is Angono Station
# table list returns the obscd and obsnm
# detail list return details about specific station / obscd


# ~~~~~~~~~~~~~~    GET LIST OF DATE/TIME STRINGS  ~~~~~~~~~~~~~~~
    # Get data from ajax
    # Have to iterate through detData every 1 day backwards since data is from present to 24hrs before( 0:00 to 23:50)
    # Start data from site: 2021-04-08 11:40 so I will iterate from today 23:50 to 2021-04-09 00
    # today = date.today().strftime("%Y%m%d")

# ---------- FOR 1-DAY DATA DOWNLOAD -----------
#Can be edited to collect historical data for a certain time period

today = date.today() #inclusive

dates_list = []

todayStr = today.strftime("%Y%m%d")
todayStr = todayStr + "2350"
dates_list.append(todayStr)
print(dates_list)


# ~~~~~~~~~~~~~~    GET STATION INFO   ~~~~~~~~~~~~~~~

tabData = {'ymdhm' : dates_list[0]}
response = requests.post(tabList_URL, data=tabData)
raw_stations =  response.json()

# ~~~~~~~~~~~~~~    WRITE STATION DETAILS TO A FILE  ~~~~~~~~~~~~~~~


cwd = r'C:\Users\USERNAME\PAGASA'
stationInfo_path = cwd + '\\Data\\' + 'StationInfo.csv'

# ~~~~~~~~~~~~~~    GET LIST OF OBSCDS FROM StationInfo.csv   ~~~~~~~~~~~~~~~
list_of_obscds = []
tab_cols = ['obscd', 'agctype', 'obsnm', 'alertwl', 'alarmwl', 'criticalwl']

with open(stationInfo_path, newline = '') as f:
    rr = csv.DictReader(f, fieldnames = tab_cols)
    for row in rr:
        if row['obscd'] != 'obscd': #because for some reason, the reader also reads the header 'obscd' and appends it to the list
            list_of_obscds.append(row['obscd'])

# ~~~~~~~~~~~~~~    GET WL DETAILS PER STATION  ~~~~~~~~~~~~~~~

i=0
j=0
wldata = []

stations = []
det_keys_to_delete = ['alertwl', 'alarmwl', 'criticalwl', 'wlchange', 'wl10m']


cols = ['obscd', 'obsnm', 'timestr', 'wl']

cwd = r'C:\Users\USERNAME\PAGASA'

for date in dates_list: # get data from site per day

    wldata = []
    for obscd in list_of_obscds:
        detData = { 'obscd' : obscd, 'ymdhm' : date  }
        r = requests.post(detList_URL, data=detData)
        rjson = r.json() #len of rjson[i]= 144
        wldata.append(rjson)

    # make csv files named with dates
    date = date.replace('2350', '')
    csv_path = cwd+ '\\DateWaterLevels\\Data\\' + date + '.csv'

    # ----- check if data in a station is incomplete (!= 144) ---------
    station_with_missing_data = ["MISSING DATA AT FF STATIONS:"]
    countOf_total_data = 0
    for data_day in wldata:
        countOf_total_data += len(data_day)
        check = len(data_day)
        missing = 144 - check
        station_missingCount = data_day[1]['obsnm'] + "(" + str(missing) + ")"
        if check != 144:
            station_with_missing_data.append(station_missingCount)

    print(station_with_missing_data) #just to debug/check
    print(countOf_total_data,"/3312")

    with open(csv_path, 'a', newline='') as f:
        wr = csv.writer(f)
        wr.writerow(station_with_missing_data)
    # -----------------------------------------------------------------

    # ------------------- save data to csv -------------------
    with open(csv_path, 'a', newline='') as f:
        wr = csv.DictWriter(f, fieldnames = cols)
        wr.writeheader()

    for data_day in wldata: #data_day = data per station for the whole day

        station_code = list_of_obscds[x]
        new_data = []
        for data_station in data_day:
            for key in det_keys_to_delete: #clean data
                data_station.pop(key)
            coded_station = {'obscd' : station_code} #add obscd
            coded_station.update(data_station   )
            new_data.append(coded_station)

        with open(csv_path, 'a', newline='') as f: #save to csv
            wr = csv.DictWriter(f, fieldnames = cols)
            wr.writerows(new_data)
