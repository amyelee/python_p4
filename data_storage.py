import os
import pandas as pd
import alpaca_trade_api as tradeapi
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta


def get_historical_alpaca_data(api, symbol, timeframe, start, end):
    """
    Fetch historical price data from Alpaca.
    """
    data = api.get_bars(symbol, timeframe, start=start, end=end).df
    return data

def clean_min_bars(data):
    """
    Clean up the minute bars data.
    """
    # convert index to Chicago timezone
    data.index = data.index.tz_convert('America/Chicago')
    # filter for only trading hours
    data = data.between_time('08:30', '15:00')
    data = data.dropna()
    return data

def save_data(data, symbol, date):
    """
    Save the data to a Parquet file.
    """
    data.to_parquet(f'{symbol}_{date}.parquet')

# function to send email if data does not exist
def send_email():
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    sender_email = 'amy9.lee@gmail.com'
    sender_password = '...' # commented out for security
    recipient_email = 'amyelee@uchicago.com'
    subject = 'Data Save Error'
    body = 'The data was not saved properly.'

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        print('Email sent successfully.')
    except Exception as e:
        print(f'Failed to send email: {e}')

def check_data_exists(symbol, timeframe, base_path=""):
    """
    Check if the data we added exists and if it has expected data in it
    """
    try:
        data = pd.read_csv(base_path + f'{symbol}_{timeframe}.parquet')
    except FileNotFoundError:
        return False
    if data.empty:
        return False
    
def fetch_and_store_data(api, symbol, timeframe, date):
    """
    Get the data for the given symbol, timeframe, and date
    """
    if check_data_exists(symbol, timeframe):
        return

    data = get_historical_alpaca_data(api, symbol, timeframe, start=date, end=date)
    if not data.empty:
        data = clean_min_bars(data)
        save_data(data, symbol, date)
        if check_data_exists(symbol, timeframe):
            send_email()
    else:
        # this is to skip weekends and holidays
        # in real life, the company would have a personal calendar to check for market holidays/weekends
        print(f"No data for {symbol} on {date}.")
    

if __name__ == '__main__':
    # ============================
    # Set the Alpaca API key and secret
    api_key = 'PKO6SDSYV5QF031PCVI5'
    api_secret = '0CnP1cSZtiQkJRSHjOce5zxjh69MA3UmyUGgFMwM'
    base_url = 'https://paper-api.alpaca.markets' 
    api = tradeapi.REST(api_key, api_secret, base_url, api_version='v2')
    
    # ============================
    # Fetch and store data
    symbol = 'TSLA'
    timeframe = '1Min'
    date = pd.Timestamp.now().date() - timedelta(days=1)
    
    os.chdir(f'/Users/amylee/Desktop/Financial Computing/Project/p4/data/{symbol}')
    fetch_and_store_data(api, symbol, timeframe, date)
    print(f"Data for {symbol} on {date} has been saved.")

    # ============================
    ## back log data
    # date_range = pd.date_range(start='2023-11-22', end='2024-11-22')
    # date_list = date_range.strftime('%Y-%m-%d').tolist()
    # for date in date_list:
    #     fetch_and_store_data(api, symbol, timeframe, date)
    #     print(f"Data for {symbol} on {date} has been saved.")
