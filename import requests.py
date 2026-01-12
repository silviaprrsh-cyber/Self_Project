import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import re
import datetime
import random
def get_gas_price():
    url = "https://www.gasbuddy.com/gasprices/alberta/red-deer"
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"}
    req = requests.get(url,headers=headers)
    if req.status_code != 200:
        return []
    soup = BeautifulSoup(req.text,"html.parser")
    station_card = soup.find_all("div",class_=re.compile("GenericStationListItem-module__stationListItem"))
    print(f"find{len(station_card)}station")
    gas_data = []
    for station in station_card:
        try:
            station_name = station.find("h3",class_=re.compile("StationDisplay-module__stationNameHeader"))
            if not station_name:
                continue
            name = station_name.text
            station_address = station.find("div",class_=re.compile("StationDisplay-module__address"))
            if not station_address:
                continue
            address = station_address.text
            station_price = station.find("span",class_=re.compile("StationDisplayPrice-module__price"))
            if station_price:
                p_price = station_price.text
                if any(i.isdigit() for i in p_price):
                    price = float(p_price.replace("¢","").strip())
                    gas_data.append({"Station":name,"Address":address,"Price":price})
                    print(f"GET IT!{name}:{price}")
        except Exception as e:
            print(f"Something wrong in the try:{e}")
            continue
    return gas_data
if __name__ == "__main__":
    print(f"Fuel Price Check ON!") 
    data = get_gas_price()
    if data:
        today = datetime.datetime.now().strftime("%Y_%m_%d")
        filename = f"red_deer_{today}_gas_price.csv"
        df = pd.DataFrame(data)
        df_sorted = df.sort_values(by="Price")
        print(f"find {len(df)} station")
        print(df_sorted)
        df_sorted.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"Today gas price save as {filename}")
    else:
        print(f"FUCK! NO DATA TODAY!")
