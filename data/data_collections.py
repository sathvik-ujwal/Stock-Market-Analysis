import yfinance as yf
import pandas as pd

stock_symbol = 'AAPL'
start_date = '2019-01-01'
end_date = '2024-01-01'

data = yf.download(stock_symbol, start=start_date, end=end_date)

data.to_csv('historical_stock_data.csv')

print("Historical data for", stock_symbol, "has been downloaded and saved as historical_stock_data.csv")
