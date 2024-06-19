from concurrent.futures import ALL_COMPLETED

import json
import datetime
import requests
import math
import pandas as pd
import urllib.parse
import concurrent.futures
import logging
import csv
from datetime import datetime, timedelta
import time


BASE_URL = 'https://www.nseindia.com/'
url_template = 'https://www.nseindia.com/api/historical/securityArchives?from={}&to={}&symbol={}&dataType=priceVolumeDeliverable&series=ALL'
 # Path to your file
historical_file = '../data/historical_output.csv'
equity_csv_file_path = '../data/EQUITY_L.csv'

#logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

def readEquityList():
    symbols = []
    # Open the CSV file and read the 'SYMBOL' column
    with open(equity_csv_file_path, mode='r') as csvfile:
        # Create a CSV reader object
        csvreader = csv.DictReader(csvfile)
    
        # Iterate over each row in the CSV
        for row in csvreader:
        # Append the symbol to the symbols list
            symbols.append(row['SYMBOL'])

    # Now you have all the symbols in the list 'symbols'
    return symbols

def get_headers():
    return {
        "Host": "www1.nseindia.com",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www1.nseindia.com/products/content/equities/equities/eq_security.htm",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With",
        'Content-Type': 'application/start_date-www-form-urlencoded; charset=UTF-8'
    }


def get_adjusted_headers():
    return {
        'Host': 'www.nseindia.com',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:85.0) Gecko/20100101 Firefox/85.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'X-Requested-With': 'XMLHttpRequest',
        'DNT': '1',
        'Connection': 'keep-alive',
    }


def fetch_cookies():
    response = requests.get(BASE_URL, timeout=30, headers=get_adjusted_headers())
    if response.status_code != requests.codes.ok:
        print("Fetched url: %s with status code: %s and response from server: %s" % (
            BASE_URL, response.status_code, response.content))
        raise ValueError("Please try again in a minute.")
    print('Cookie fetched')
    return response.cookies.get_dict()


def fetch_url(url, cookies):
    """
        This is the function call made by each thread. A get request is made for given start and end date, response is
        parsed and dataframe is returned
    """

    max_attempts = 5
    attempt = 0

    while attempt < max_attempts:
        try:
            response = requests.get(url, timeout=30, headers=get_adjusted_headers(), cookies=cookies)
            if response.status_code == requests.codes.ok:
                json_response = json.loads(response.content)
                return pd.DataFrame.from_dict(json_response['data'])
            
            else:
                print("Fetched url: %s with status code: %s and response from server: %s" % (
                BASE_URL, response.status_code, response.content))
                #raise ValueError("Please try again in a minute.")
                attempt += 1
                if attempt < max_attempts:
                    print(f"Retrying in 30 seconds... (Attempt {attempt}/{max_attempts})")
                    time.sleep(30)
                    cookies = fetch_cookies()
                else:
                    print("Maximum attempts reached. Giving up.")
                    return pd.DataFrame()
                
        except Exception as e:
            print(f"An error occurred: {e}")
            return pd.DataFrame()
        

def generate_dataframe():
    # Replace with your actual dataframe generation logic
    return pd.DataFrame({'data': [1, 2, 3]})    

def readNSEData(symbol_name, from_date, to_date, cookies):
    url = url_template.format(from_date, to_date, symbol_name)
    print(symbol_name)
    data = fetch_url(url, cookies)
    #print(data)
    return data
    #return generate_dataframe()


def getAllData(from_date, to_date, file_path = historical_file):
    # Create or clear the file before starting the loop
    with open(file_path, 'w') as f:
        pass

    symbols = readEquityList()
    chunk_df = pd.DataFrame()
    cookies = fetch_cookies()

    for i, symbol in enumerate(symbols):
        data = readNSEData(symbol, from_date, to_date, cookies)
        if data.empty:
            print('Could not fetch Data for Symbol: ' + symbol)
        else:
            chunk_df = pd.concat([chunk_df, data], ignore_index=True)
            # Check if the chunk has reached 100 rows or if we are at the last DataFrame
            #if i >= 10:
            if i % 100 == 0 or i == len(symbols) - 1:
                # Open the file in append mode and write the chunk
                with open(file_path, 'a') as f:
                    # If the file is being created, write header, else skip it
                    header = f.tell() == 0
                    chunk_df.to_csv(f, sep=';', mode='a', header=header, index=False)
                # Clear the chunk DataFrame and cookies after writing to the file
                chunk_df = pd.DataFrame()
                cookies = fetch_cookies()



def stockWiseData(symbol, from_date, to_date, stock_file = '../data/temp.csv'):
    # Create or clear the file before starting the loop
    with open(stock_file, 'w') as f:
        pass
    cookies = fetch_cookies()
    data = readNSEData(symbol, from_date, to_date, cookies)
    with open(stock_file, 'a') as f:
        header = f.tell() == 0
        data.to_csv(f, sep=';', mode='a', header=header, index=False)


def test123(from_date, to_date):
    symbols = readEquityList()
    chunk_df = pd.DataFrame()
    cookies = fetch_cookies()

    for i, symbol in enumerate(symbols):
        print(i, symbol)

def tests():
    from_date = '01-06-2024'
    to_date = '14-06-2024'
    symbol_name = 'FLUOROCHEM'
    cookies = fetch_cookies()
    print(readNSEData(symbol_name, from_date, to_date, cookies))


def main_call():
    # Get today's date
    today = datetime.now()
    # Format today's date as 'DD-MM-YYYY'
    to_date = today.strftime('%d-%m-%Y')    
    
    
    #for historical data
    weeks_before = 2
    days_before = weeks_before * 7  # 24 weeks * 7 days/week
    from_date = (today - timedelta(days=days_before)).strftime('%d-%m-%Y')
    getAllData(from_date,to_date)


    #for daily/weekly data
    #sv_filename = f"../data/data_{to_date}.csv"
    #days_before = 1 
    #from_date = (today - timedelta(days=days_before)).strftime('%d-%m-%Y')
    #getAllData(from_date,to_date, sv_filename)


#from_date = '01-06-2024'
#to_date = '14-06-2024'
#symbol_name = 'FLUOROCHEM'
#cookies = fetch_cookies()
#print(readNSEData(symbol_name, from_date, to_date, cookies))

#stock_file_name = '../data/FLUOROCHEM.csv'
#stockWiseData(symbol_name, from_date, to_date, stock_file_name)

main_call()