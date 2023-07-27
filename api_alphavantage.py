# import necessary libraries
import pandas as pd
import requests
import json
from typing import Union, List

# Configure API parameters

#Advantage api configuration
base_url = 'https://www.alphavantage.co/query'
token = "OYDV8S8KFCSH3B35"
#Parameters for stocks on market data
function = 'TIME_SERIES_INTRADAY'
symbol= 'AAPL'
interval = '15min'
adjusted=True
month='2023-01'
size = 'full'

#fix the params
parameters_market_data = {'function':function, 'symbol':symbol, 'interval':interval, 
            'adjusted':adjusted,'outputsize':size,'apikey':token, 'month':month, }
#call the API 
r = requests.get(base_url,params=parameters_market_data)
#Pass the json response to pandas dataframe
data = r.json()['Time Series (15min)']
data_df = pd.DataFrame.from_dict(data, orient="index")

#function to retrieve news and sentiment analysis on the same symbol specified before

def getSentiment(
    symbol: str,
    function_sentiment: str,
    topics: Union[str, List[str]],
    time_from: Union[str, int],
    time_to: Union[str, int]
):
    url = 'https://finnhub.io/api/v1/news-sentiment'
    
    # Convert topics to a comma-separated string if it is a list of strings
    if isinstance(topics, list):
        topics = ','.join(topics)
    
    # Convert time_from and time_to to strings if they are integers
    time_from = str(time_from)
    time_to = str(time_to)
    
    parameters_news_sentiment_data = {
        'function': function_sentiment,
        'tickers': symbol,
        'topics': topics,
        'time_from': time_from,
        'time_to': time_to,
        'apikey': token
    }
    r = requests.get(base_url, params=parameters_news_sentiment_data)
    data = r.json()
    data_feed = data['feed']
    data_sentiment = []
    for i in data_feed:
        for item in i['ticker_sentiment']:
            if item['ticker'] == symbol:
                data_sentiment.append({
                    'time_published': i['time_published'],
                    'source_domain': i['source_domain'],
                    'relevance_score': item['relevance_score'],
                    'ticker_sentiment_label': item['ticker_sentiment_label']
                })
    return data_sentiment
