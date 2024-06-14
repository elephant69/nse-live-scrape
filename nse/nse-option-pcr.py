import json
import requests
import pandas as pd

def print_json_structure(d, indent=0):
    for key, value in d.items():
        print('  ' * indent + str(key))
        if isinstance(value, dict):
            print_json_structure(value, indent+1)
        elif isinstance(value, list) and value:
            print('  ' * (indent+1) + 'List of:')
            if isinstance(value[0], dict):
                print_json_structure(value[0], indent+2)

def calculate_pcr_for_date(json_data, target_expiry_date):
    # Load the JSON data if it's a string, otherwise assume it's already a dict
    if isinstance(json_data, str):
        data = json.loads(json_data)
    else:
        data = json_data


    # Iterate over each item in the data
    for item in data['data']:
        if item['expiryDate'] == target_expiry_date:
            print(item)
            totCE = data['CE']['totOI']
            totPE = data['PE']['totOI']
            return (totPE/totCE)

    return 0


def calculate_pcr_for_all_date(json_data):
    # Load the JSON data if it's a string, otherwise assume it's already a dict
    if isinstance(json_data, str):
        data = json.loads(json_data)
    else:
        data = json_data

    # Initialize a dictionary to hold sum and count of bidPrices for each expiryDate
    total_PE_changeINOI = {}
    total_CE_changeINOI = {}

    # Loop through each item and sum the bidPrices for each expiryDate
    for item in data['data']:
        expiry_date = item['expiryDate']
        if 'PE' in item and 'changeinOpenInterest' in item['PE']:
            if expiry_date not in total_PE_changeINOI:
                total_PE_changeINOI[expiry_date] = 0
            total_PE_changeINOI[expiry_date] += item['PE']['changeinOpenInterest']
        elif 'CE' in item and 'changeinOpenInterest' in item['CE']:
            if expiry_date not in total_CE_changeINOI:
                total_CE_changeINOI[expiry_date] = 0
            total_CE_changeINOI[expiry_date] += item['CE']['changeinOpenInterest']

    PCR = {}

    for expiry_date in total_PE_changeINOI:
        # Ensure there is a corresponding changeinOpenInterest and it's not zero to avoid division by zero
        if expiry_date in total_CE_changeINOI and total_CE_changeINOI[expiry_date] != 0:
            PCR[expiry_date] = total_PE_changeINOI[expiry_date] / total_CE_changeINOI[expiry_date]
        else:
            PCR[expiry_date] = None  # None or some other placeholder to indicate undefined ratio

    return PCR


def readNSEData(symbol):
    url = 'https://www.nseindia.com/api/option-chain-indices?symbol='+symbol
    headers = {
    'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
    'accept-encoding' : 'gzip, deflate, br',
    'accept-language' : 'en-US,en;q=0.9'
    }
    response = requests.get(url, headers=headers).content
    data = json.loads(response.decode('utf-8'))

    return data


def getCurrentPCR(symbol):
    data = readNSEData(symbol)
    totCE = data['filtered']['CE']['totOI']
    totPE = data['filtered']['PE']['totOI']
    return(totPE/totCE)

    return(calculate_pcr_for_date(data2,'13-Jun-2024'))

def getAllPCR(symbol):
    data = readNSEData(symbol)
    data2 = data['records']
    #first_10_items = data2['data'][:10]

    # Print the first 10 items
    #for item in first_10_items:
    #    print(item) 
    pcrs = calculate_pcr_for_all_date(data2)
    for date, ratio in pcrs.items():
        if ratio is not None:
            print(f"  {date}: {ratio}")
        else:
            print(f"  {date}: Ratio not defined (changeinOpenInterest is zero or not present)")

#getAllPCR('NIFTY')
pcr = getCurrentPCR('NIFTY')
print('PCR = ', pcr)   

