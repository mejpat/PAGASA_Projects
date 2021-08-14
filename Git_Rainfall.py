import json
import requests
from datetime import date
from datetime import timedelta
import csv
import os

tabList_URL = "http://121.58.193.173:8080/rainfall/table_list.do"
#data needed
#ymdhm : "202106092350"
#the date/time format is YYYYMMDDHHMM

detList_URL = "http://121.58.193.173:8080/rainfall/detail_list.do"
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
#today = date(2021, 7, 9) #for testing
dates_list = []

todayStr = today.strftime("%Y%m%d")
todayStr = todayStr + "2350"
dates_list.append(todayStr)
print(dates_list)

# 1~~~~~~~~~~~~~~    GET STATION INFO   ~~~~~~~~~~~~~~~

tabData = {'ymdhm' : dates_list[0]}
response = requests.post(tabList_URL, data=tabData)
raw_stations =  response.json()


# ~~~~~~~~~~~~~~    WRITE STATION DETAILS TO A FILE  ~~~~~~~~~~~~~~~

cwd = r'C:\Users\USERNAME\PAGASA'
stationInfo_path = cwd + '\\Data\\' + 'StationInfo.csv'

# ~~~~~~~~~~~~~~    LIST OF OBSCDS   ~~~~~~~~~~~~~~~

tab_keys_to_delete = ["timestr", "rf", "rf30m", "rf01h" , "rf03h", "rf06h", "rf12h", "rf24h"]
new_stations = []
list_of_obscds = []

tab_cols =  ["obscd","agctype","obsnm"]

# ~~~~~~~~~~~~~~    GET LIST OF OBSCDS FROM StationInfo.csv   ~~~~~~~~~~~~~~~
list_of_obscds = []
list_of_obsnm = []  #used in the for loop checking for missing data
tab_cols =  ["obscd","agctype","obsnm"]

with open(stationInfo_path, newline = '') as f:
    rr = csv.DictReader(f, fieldnames = tab_cols)
    for row in rr:
        if row['obscd'] != 'obscd':
            list_of_obscds.append(row['obscd'])
            list_of_obsnm.append(row['obsnm'])


# ~~~~~~~~~~~~~~    GET WL DETAILS PER STATION  ~~~~~~~~~~~~~~~
i=0
j=0
wldata = []

stations = []

cols = ['obscd', 'obsnm', 'timestr', 'rf' , 'rfday']

cwd = r'C:\Users\USERNAME\PAGASA'


for date in dates_list:
    wldata = []
    for obscd in list_of_obscds: # get data from each station/obscd
        detData = { 'obscd' : obscd, 'ymdhm' : date  }
        r = requests.post(detList_URL, data=detData)
        rjson = r.json() #len of rjson[i]= 144
        wldata.append(rjson)

    date = date.replace('2350', '')
    csv_path = cwd+ '\\DateRainfall\\Data\\' + date + '.csv'

    # ----- check if data in a station is incomplete (!= 144) ---------
    station_with_missing_data = []
    countOf_total_data = 0

    for index, data_day in enumerate(wldata):
        if len(data_day) == 0:
            missing = 144
            station_missingCount = list_of_obsnm[index] + " (" + str(missing) + ")"
            station_with_missing_data.append(station_missingCount)
        else:
            countOf_total_data += len(data_day)
            check = len(data_day)
            missing = 144 - check
            station_missingCount = data_day[1]['obsnm'] + " (" + str(missing)  + ")"
            if check != 144:
                station_with_missing_data.append(station_missingCount)
    countOf_Station_MissingData = len(station_with_missing_data)
    str_countOf_Station_MissingData = 'MISSING DATA AT FF STATIONS' + " (" + str(countOf_Station_MissingData) + "):"
    station_with_missing_data.insert(0,str_countOf_Station_MissingData)

    print(station_with_missing_data) #just to debug/check
    print(countOf_total_data,"/ 4320")
    print("STATION ---- RF24")

    #string_stations =", ".join(station_with_missing_data)
    with open(csv_path, 'a', newline='') as f:
        wr = csv.writer(f)
        wr.writerow(station_with_missing_data)
    # -----------------------------------------------------------------

    # ------------------ SAVE DATA OF EACH STATION TO CSV ------------------
    with open(csv_path, 'a', newline='') as f:
        wr = csv.DictWriter(f, fieldnames = cols)
        wr.writeheader()

    # wldata = data of ALL STATIONS for the whole day
    for index, data_day in enumerate(wldata): #data_day = data of each station for the whole day
        station_code = list_of_obscds[x]
        new_data = []
        for data_station in data_day: # data of each station
            #data_station["obscd"] = station_code #add obscd
            coded_station = {'obscd' : station_code} #add obscd
            coded_station.update(data_station)
            new_data.append(coded_station)
            # ---FUTURE IMPROVEMENTS------- PRINT HIGHEST RF WITH TIME FOR EACH STATION
        if len(data_day) == 0:
            print(list_of_obsnm[index], "---- NO DATA")
        else:
            print(list_of_obsnm[index], "----", data_day[0]['rfday'])


        with open(csv_path, 'a', newline='') as f: #save to csv
            wr = csv.DictWriter(f, fieldnames = cols)
            wr.writerows(new_data)
