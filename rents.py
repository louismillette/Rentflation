import pandas as pd
import numpy as numpy
import os
import time
import sys
from geopy.geocoders import Nominatim


class PreProcess():
    def __init__(self) -> None:
        self.df = pd.read_csv('City_zori_uc_sfrcondomfr_sm_month.csv') # from zillow: https://files.zillowstatic.com/research/public_csvs/zori/City_zori_uc_sfrcondomfr_sm_month.csv?t=1716921987
        self.df_inflation = pd.read_csv('inflation data.csv') # https://data.bls.gov/pdq/SurveyOutputServlet All items less food and energy
        self.df_latlon = pd.read_csv('CityLatLon.csv') #
        self.running_total = 0
        self.total = 0
        self.start = 3525
        self.terminal = 1000000

    def processData(self):
        self.df["RegionName"] = self.df["RegionName"] + ", " + self.df["State"]
        self.df = self.df.drop(["State", "Metro", "CountyName"], axis=1)
        df_dates = pd.DataFrame(self.df.iloc[:, 5:].columns, columns=['Dates'])
        df_dates['key'] = 0
        print(f'[+] Pulled {df_dates.shape[0]} Dates')

        # pull city data
        df_cities = pd.DataFrame()
        df_cities['Cities'] = self.df.iloc[:, 2]
        self.running_total = 0
        self.total = df_cities.shape[0]
        self.terminal = self.total
        print('[+] Pulling city lat & lon please wait... ')

        df_cities = df_cities.merge(self.df_latlon, how="outer", on="Cities")
        df_cities['key'] = 0
        print(f'[+] Pulled {df_cities.shape[0]} cities')

        print('[+] Pulling prices please wait... ')
        df_full = df_dates.merge(df_cities, how="outer", on="key")
        df_full = df_full.drop("key", axis=1)
        self.total = df_full.shape[0]
        df_full['Price'] = df_full.apply(self.lookupPrice, axis=1)

        # Exclude cities with no data on 01/31/15.  These cannot be accurately compared to cities with a full set of data.
        self.excludedCities = df_full[(df_full['Dates'] == '01/31/15') & (pd.isnull(df_full['Price']))]["Cities"]
        df_full = df_full[~df_full["Cities"].isin(self.excludedCities)]
        df_full = df_full.dropna(subset=['Price'])

        # Merge city retnal prices and inflation data
        print(f'[+] Pulled {df_full.shape[0]} prices')
        df_full = df_full.merge(self.df_inflation, how="outer", on="Dates")
        print(f'[+] Pulled {self.df_inflation.shape[0]} Inflation entries, merged into {df_full.shape[0]} data points')

        # output
        df_full.to_csv('Rentflation.csv', index=False)
        cwd = os.getcwd()
        print(f'[+]  Output file: {os.path.join(cwd, "Rentalflation.csv")}')

    def animate(self, iteration, total):
        percent = ("{0:.1f}").format(100 * (iteration / float(total)))
        filled_length = int(50 * iteration // total)
        bar = '█' * filled_length + '-' * (50 - filled_length)
        sys.stdout.write('\r|%s| %s%% Complete (%s/%s)' % (bar, percent, iteration, total))
        sys.stdout.flush()

    def lookupPrice(self, row):
        date = row['Dates']
        city = row['Cities']
        price = self.df.loc[self.df['RegionName'] == city, date]
        self.running_total += 1
        self.animate(self.running_total, self.total)
        try:
            return float(price)
        except:
            # pleanty of null data that also must be included
            return None

    def createLatLonCSV(self,df_cities):
        df_cities[['Lat', 'Lon']] = df_cities.apply(self.getLatLon, axis=1).apply(pd.Series)
        df_cities.to_csv(f'CityData {self.start} to {self.terminal}.csv', index=False)
        cwd = os.getcwd()
        print(f'[+]  Output file: {os.path.join(cwd, "CityData.csv")}')

    def getLatLon(self, row):
        if self.running_total == self.terminal:
            return 0,0
        elif self.start > self.running_total:
            self.running_total += 1
            return 0,0
        cityName = row['Cities']
        geolocator = Nominatim(user_agent="tester3")
        try:
            location = geolocator.geocode(cityName)
        except:
            self.terminal = self.running_total
            return 0,0
        self.running_total += 1
        self.animate(self.running_total, self.total)
        try:
            return location.latitude, location.longitude
        except:
            print(f'{cityName} has no lat lon data')
            return 0, 0
        


if __name__ == '__main__':
    P = PreProcess()
    P.processData()