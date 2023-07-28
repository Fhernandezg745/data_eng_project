# importo las librerias necesarias
import os
import requests
import json
from datetime import datetime
from typing import Union, List
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

#Advantage api configuration
base_url = os.environ.get('BASE_URL')
token = os.environ.get('API_TOKEN')

#Primero: funcion para traer market data, en particular serie intradiaria del stock que necesito
def intraday_stock_serie(symbol:str, interval:str):   
    function = 'TIME_SERIES_INTRADAY'
    adjusted=True
    extended_hours=False
    size = 'compact'
    parameters_market_data = {'function':function, 'symbol':symbol, 'interval':interval, 'extended_hours':extended_hours,
            'adjusted':adjusted,'outputsize':size,'apikey':token }
    try:
        r = requests.get(base_url, params=parameters_market_data)
        r.raise_for_status()  
        data = r.json()[f'Time Series ({interval})']
        return data
    except requests.exceptions.HTTPError as http_err:
        print(f"error: {http_err}")
        return {}
    except Exception as err:
        print(f"error: {err}")
        return {}


#Segundo: funcion para traer noticias relacionadas a ese stock
def getSentiment(
    symbol: str,
    topics: Union[str, List[str]]
):
    # Convierto topics en un solo string si vino en una lista de strings
    if isinstance(topics, list):
        topics = ','.join(topics)
        
    parameters_news_sentiment_data = {
        'function': 'NEWS_SENTIMENT',
        'tickers': symbol,
        'topics': topics,
        'apikey': token
    }
    
    try:
        r = requests.get(base_url, params=parameters_news_sentiment_data)
        r.raise_for_status() 
        data = r.json()
        data_feed = data['feed']
        data_sentiment = []
        for i in data_feed:
            for item in i['ticker_sentiment']:
                if item['ticker'] == symbol:
                    # Formateo time_published 
                    time_published = datetime.strptime(i['time_published'], '%Y%m%dT%H%M%S')
                    formatted_time_published = time_published.strftime('%Y-%m-%d %H:%M')
                    data_sentiment.append({
                        'ticker': item['ticker'],
                        'time_published': formatted_time_published,
                        'source_domain': i['source_domain'],
                        'relevance_score': item['relevance_score'],
                        'ticker_sentiment_label': item['ticker_sentiment_label']
                    })
        return data_sentiment
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return []
    except Exception as err:
        print(f"Other error occurred: {err}")
        return []


# 3 Unifico las funciones de market data y news data en una sola 
def get_stock_data(tickers, interval, topics):
    stock_data_frames = {}
    
    for ticker in tickers:
        # Traigo intraday stock data
        intraday_data = intraday_stock_serie(ticker, interval)
        
        # Traigo sentiment data
        sentiment_data = getSentiment(ticker, topics)
        
        # Convierto en pandas dataframe
        df_intraday = pd.DataFrame(intraday_data).transpose()
        df_intraday.index = pd.to_datetime(df_intraday.index)
        df_sentiment = pd.DataFrame(sentiment_data)
        
        # Guardo los df en diccionarios
        stock_data_frames[ticker] = {
            'intraday_data': df_intraday,
            'sentiment_data': df_sentiment
        }
    return stock_data_frames

