# importo las librerias necesarias
import os
import requests
import json
from datetime import datetime
from typing import Union, List
import pandas as pd
from dotenv import load_dotenv
import sqlalchemy as sa
load_dotenv()

#Creo una clase para agarrar los errores que puedan venir en la API
class APIRequestError(Exception):
    def __init__(self, status_code, message, function_name):
        self.status_code = status_code
        self.message = message
        self.function_name = function_name
        super().__init__(f"HTTP error {self.status_code} occurred in {self.function_name}: {self.message}")

#Primero: creo funcion para traer market data, en particular serie intradiaria del stock que necesito
def intraday_stock_serie(symbol:str, interval:str):   
    endpoint = 'TIME_SERIES_INTRADAY'
    adjusted=True
    extended_hours=False
    size = 'compact'
    parameters_market_data = {'function':endpoint, 'symbol':symbol, 'interval':interval, 'extended_hours':extended_hours,
            'adjusted':adjusted,'outputsize':size,'apikey':token }
    try:
        r = requests.get(base_url, params=parameters_market_data)
        r.raise_for_status()  
        data = r.json()
        if "Error Message" in data:
            error_message = data["Error Message"]
            raise APIRequestError(r.status_code, error_message, "intraday_stock_serie")
        else:
            data = data[f'Time Series ({interval})']
            return data
    except requests.exceptions.HTTPError as http_err:
        raise APIRequestError(http_err.response.status_code, http_err, "intraday_stock_serie")
    except Exception as err:
        raise APIRequestError(500, str(err), "intraday_stock_serie")


#Segundo: creo funcion para traer noticias relacionadas a ese stock

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
        raise APIRequestError(http_err.response.status_code, http_err, "getSentiment")
    except Exception as err:
        raise APIRequestError(500, str(err), "getSentiment")




# 3 Unifico las funciones de market data y news data en una sola y creo las tablas en redshift
def get_stock_data(tickers, interval, topics):
    successful_rows = {}
    # Get database connection parameters
    DB_NAME = os.environ.get('DB_NAME')
    DB_USER = os.environ.get('DB_USER')
    DB_PWD = os.environ.get('DB_PWD')
    DB_PORT = os.environ.get('DB_PORT')
    DB_HOST = os.environ.get('DB_HOST')
    dbschema = f'{DB_USER}'

    # Create the connection engine outside the loop
    conn = sa.create_engine(
        f"postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        connect_args={'options': f'-csearch_path={dbschema}'}
    )
    
    for ticker in tickers:
        try:
            # Traigo intraday stock data
            intraday_data = intraday_stock_serie(ticker, interval)
            
            # Traigo sentiment data
            sentiment_data = getSentiment(ticker, topics)
            
            # Convierto en pandas dataframe los precios de intraday stock
            df_intraday = pd.DataFrame.from_dict(intraday_data, orient='index')
            df_intraday.columns = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
            df_intraday.reset_index(inplace=True)
            df_intraday.rename(columns={'index': 'date'}, inplace=True)
            df_intraday['date'] = pd.to_datetime(df_intraday['date'])
            
            intraday_table_name = f'stock_intraday_prices_{ticker}'
            
            #Creo tabla de precios en redshift
            df_intraday.to_sql(intraday_table_name, conn, index=False, if_exists='replace')
            # cantidad de rows de intraday 
            num_rows_updated_intraday = len(df_intraday)

            #Voy con el dataframe de news sentiment
            df_sentiment = pd.DataFrame(sentiment_data)
            sentiment_table_name = f'stock_sentiment_{ticker}'
            
            #Creo tabla de sentiment en redshift
            df_sentiment.to_sql(sentiment_table_name, conn, index=False, if_exists='replace')
            # cantidad de rows de sentiment
            num_rows_updated_sentiment = len(df_sentiment)

            
            successful_rows[ticker] = {
                'intraday_data': num_rows_updated_intraday,
                'sentiment_data': num_rows_updated_sentiment
            }
            
        except APIRequestError as api_err:
            print(f"{api_err.function_name}: API Request Error - Status Code {api_err.status_code}: {api_err.message}")
            # You can handle the error based on the status code here.
            # For example, you may choose to skip the stock if the error is not recoverable.
            continue
    return successful_rows


################################################################

#Configuro los parametros de la API desde un archivo .env, establezco que voy a querer ver stocks de Apple y de IBM en intervalo de 60min
# La Api de advantage tiene un limit por default de 100 filas para los stockprices y 50 para news sentiment, pero puede customizarse 
# agregandole una fecha rango o un mes
# topics es un parametro para bucar noticias de esos temas en particular
#interval es un parametro para establecer la frecuencia del precio de la accion que queramos, en este caso cada una hora
base_url = os.environ.get('BASE_URL')
token = os.environ.get('API_TOKEN')
tickers = ['AAPL','IBM']
interval = '60min'
topics = 'technology, manufacturing, financial_markets'

################################################################
# Creo una variable donde ejecuto la funcion y guardo el resultado
data_frames_by_ticker = get_stock_data(tickers, interval, topics)

### Instancio los resultados de cada accion y de cada tabla
print(data_frames_by_ticker)
