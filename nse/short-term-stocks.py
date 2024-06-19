import pandas as pd

historical_file = '../data/historical_output.csv'
top_20_vol = '../data/top_20_vol.csv'
top_20_vol_vwap = '../data/top_20_vwap.csv'

# Read the CSV file
df = pd.read_csv(historical_file, sep=';', on_bad_lines='skip')

df['CH_TIMESTAMP'] = pd.to_datetime(df['CH_TIMESTAMP'], errors='coerce')
# Replace '-' with NaN and convert columns to float
df['COP_DELIV_QTY'] = pd.to_numeric(df['COP_DELIV_QTY'], errors='coerce').fillna(0)
df['COP_DELIV_PERC'] = pd.to_numeric(df['COP_DELIV_PERC'], errors='coerce').fillna(0)
df['    '] = pd.to_numeric(df['CH_CLOSING_PRICE'], errors='coerce').fillna(0)
df['CH_LAST_TRADED_PRICE'] = pd.to_numeric(df['CH_LAST_TRADED_PRICE'], errors='coerce').fillna(0)
df['CH_PREVIOUS_CLS_PRICE'] = pd.to_numeric(df['CH_PREVIOUS_CLS_PRICE'], errors='coerce').fillna(0)
df['VWAP'] = pd.to_numeric(df['VWAP'], errors='coerce').fillna(0)


def topStocksOnlyBasedOnVolume(df):
    # Find the last timestamp for each stock
    last_timestamps = df.groupby('CH_SYMBOL')['CH_TIMESTAMP'].max()

    aggregated_data_list = []

    # Loop through each stock and its last timestamp
    for symbol, last_date in last_timestamps.items():
        # Filter the original dataframe for the last 5 days for this stock
        mask = (df['CH_SYMBOL'] == symbol) & (df['CH_TIMESTAMP'] >= (last_date - pd.Timedelta(days=5)))
        stock_df = df.loc[mask]
    
        # Aggregate the delivery volume and delivery percentage for this stock
        stock_agg = stock_df.agg({
            'COP_DELIV_QTY': 'sum',  # Sum of delivery quantity over the last 5 days
            'COP_DELIV_PERC': 'mean'  # Average of delivery percentage over the last 5 days
        })
    
        # Add the stock symbol and last timestamp to the aggregated data
        stock_agg['CH_SYMBOL'] = symbol
        stock_agg['LAST_TIMESTAMP'] = last_date

        # Get the latest VWAP and latest closing price from the last available day
        latest_record = stock_df.loc[stock_df['CH_TIMESTAMP'].idxmax()]
        stock_agg['LATEST_VWAP'] = latest_record['VWAP']
        stock_agg['LATEST_CLOSING_PRICE'] = latest_record['CH_CLOSING_PRICE']
    
        # Append the aggregated data for this stock to the list
        aggregated_data_list.append(stock_agg)

    # Concatenate all the aggregated data into a single dataframe
    aggregated_data = pd.concat(aggregated_data_list, axis=1).transpose()
    # Reorder the columns to have 'CH_SYMBOL' and 'LAST_TIMESTAMP' first
    column_order = ['CH_SYMBOL', 'LAST_TIMESTAMP', 'COP_DELIV_QTY', 'COP_DELIV_PERC', 'LATEST_VWAP', 'LATEST_CLOSING_PRICE']
    aggregated_data = aggregated_data[column_order]

    top_stocks = aggregated_data.sort_values(by=['COP_DELIV_PERC', 'COP_DELIV_QTY'], ascending=[False, False]).head(20)
    print(top_stocks)
    return top_stocks


def topStocksBasedOnVolumeandVWAP(df):
    # Find the last timestamp for each stock
    last_timestamps = df.groupby('CH_SYMBOL')['CH_TIMESTAMP'].max()

    aggregated_data_list = []

    # Loop through each stock and its last timestamp
    for symbol, last_date in last_timestamps.items():
        # Filter the original dataframe for the last 5 days for this stock
        mask = (df['CH_SYMBOL'] == symbol) & (df['CH_TIMESTAMP'] >= (last_date - pd.Timedelta(days=5)))
        stock_df = df.loc[mask]

         # Further filter where VWAP is less than CH_CLOSING_PRICE
        stock_df = stock_df[stock_df['VWAP'] < stock_df['CH_CLOSING_PRICE']]
    
        # If there are no records left after filtering, skip this stock
        if stock_df.empty:
            continue
    
        # Aggregate the delivery volume and delivery percentage for this stock
        stock_agg = stock_df.agg({
            'COP_DELIV_QTY': 'sum',  # Sum of delivery quantity over the last 5 days
            'COP_DELIV_PERC': 'mean'  # Average of delivery percentage over the last 5 days
        })
    
        # Add the stock symbol and last timestamp to the aggregated data
        stock_agg['CH_SYMBOL'] = symbol
        stock_agg['LAST_TIMESTAMP'] = last_date

        # Get the latest VWAP and latest closing price from the last available day
        latest_record = stock_df.loc[stock_df['CH_TIMESTAMP'].idxmax()]
        stock_agg['LATEST_VWAP'] = latest_record['VWAP']
        stock_agg['LATEST_CLOSING_PRICE'] = latest_record['CH_CLOSING_PRICE']
    
        # Append the aggregated data for this stock to the list
        aggregated_data_list.append(stock_agg)

    # Concatenate all the aggregated data into a single dataframe
    aggregated_data = pd.concat(aggregated_data_list, axis=1).transpose()
    # Reorder the columns to have 'CH_SYMBOL' and 'LAST_TIMESTAMP' first
    column_order = ['CH_SYMBOL', 'LAST_TIMESTAMP', 'COP_DELIV_QTY', 'COP_DELIV_PERC', 'LATEST_VWAP', 'LATEST_CLOSING_PRICE']
    aggregated_data = aggregated_data[column_order]

    top_stocks = aggregated_data.sort_values(by=['COP_DELIV_PERC', 'COP_DELIV_QTY'], ascending=[False, False]).head(20)
    print(top_stocks)
    return top_stocks


topbyVol = topStocksOnlyBasedOnVolume(df)
topByVolandVWAP = topStocksBasedOnVolumeandVWAP(df)

topbyVol.to_csv(top_20_vol, sep=';', index=False)
