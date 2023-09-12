import os
import requests
import json
from datetime import datetime
from configparser import ConfigParser
from typing import Union, List
import pandas as pd
import sqlalchemy as sa

from airflow import DAG
from airflow.operators.python_operator import PythonOperator



#Cre una clase para error handling
class APIRequestError(Exception):
    def __init__(self, status_code, message, function_name):
        self.status_code = status_code
        self.message = message
        self.function_name = function_name
        super().__init__(f"HTTP error {self.status_code} occurred in {self.function_name}: {self.message}")


class DatabaseError(Exception):
    pass



#Primero: funcion para traer serie intradiaria en 60 min del stock que necesito

def intraday_stock_serie(symbol:str, interval:str, base_url, token):   
    endpoint = 'TIME_SERIES_INTRADAY'
    adjusted=True
    extended_hours=False
    size = 'compact'
    parameters_market_data = {'function':endpoint, 'symbol':symbol, 'interval':interval, 'extended_hours':extended_hours,
            'adjusted':adjusted,'outputsize':size,'apikey':token }
    try:
        print("llamando a la API ...")
        r = requests.get(base_url, params=parameters_market_data)
        r.raise_for_status() 
        data = r.json()
        print("Data recibida ...")
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
    
    
    
#Segundo: funcion para traer noticias relacionadas a ese stock

def getSentiment(
    symbol: str,
    topics: Union[str, List[str]],
    base_url,
    token
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

def connect_to_db(config_file_path="/opt/airflow/config/config.ini", section="RedshiftServer"):
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"El archivo de configuración '{config_file_path}' no existe.")

    # Leer la configuración desde el archivo INI
    config = ConfigParser()
    config.read(config_file_path)
    conn_data = config[section]

    # Get database connection parameters
    DB_NAME = conn_data.get('DB_NAME')
    DB_USER = conn_data.get('DB_USER')
    DB_PWD = conn_data.get('DB_PWD')
    DB_PORT = conn_data.get('DB_PORT')
    DB_HOST = conn_data.get('DB_HOST')
    dbschema = f'{DB_USER}'

    url =  f"postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # Establecer la conexión a la base de datos PostgreSQL
    conn = sa.create_engine(url,connect_args={'options': f'-csearch_path={dbschema}'})
    return conn



# 3 Unifico las funciones de market data y news data en una sola 
def create_table(tickers, engine):

    # Create the connection engine outside the loop
    print("Creando la conexion con redshift")
    conn = engine
    print("Conexion con redshift realizada con éxito")
    
    for ticker in tickers:
        print(f"Creando las tablas y conectando a la API para extraer data de {ticker} ")
        try:
            
            intraday_table_name = f'stock_intraday_prices_{ticker}'
            
            print(f"Creando tabla de {ticker} en redshift")
            #Creo tabla de precios en redshift
            conn.execute(f"""
                DROP TABLE IF EXISTS {intraday_table_name};
                CREATE TABLE {intraday_table_name} (
                    date TIMESTAMP,
                    open_price FLOAT,
                    high_price FLOAT,
                    low_price FLOAT,
                    close_price FLOAT,
                    volume INT
                )
                DISTKEY(date)
                SORTKEY(date);
                COMMIT;
            """)
    
            print("Tabla de price creada... ")
            #Voy con el dataframe de news sentiment
            
            sentiment_table_name = f'stock_sentiment_{ticker}'
            
            #Creo tabla de sentiment en redshift
            conn.execute(f"""
            DROP TABLE IF EXISTS {sentiment_table_name};
            CREATE TABLE {sentiment_table_name}  (
                id INT IDENTITY(1,1) PRIMARY KEY,
                ticker VARCHAR,
                time_published TIMESTAMP,
                source_domain VARCHAR,
                relevance_score VARCHAR,
                ticker_sentiment_label VARCHAR
            )
            DISTKEY(time_published)
            SORTKEY(time_published);
            COMMIT;
            ;
            """)
            print("Tabla de sentiment creada... ")
            
        except Exception as e:
            raise DatabaseError(f"Failed to connect to the database: {str(e)}")
            # You can handle the error based on the status code here.
            # For example, you may choose to skip the stock if the error is not recoverable.
            continue
    return f"Tablas para {tickers} creadas"




def fill_table(tickers, interval, topics, engine, config_file_path="/opt/airflow/config/config.ini", section="AlphavantageAPI"):
        if not os.path.exists(config_file_path):
            raise FileNotFoundError(f"El archivo de configuración '{config_file_path}' no existe.")

        #creo conn
        print("Conectando con Redshift ... ")
        conn=engine
        print("Conexion establecida OK")
        #Conecto con API
        config = ConfigParser()
        config.read(config_file_path)
        conn_data = config[section]
        base_url = conn_data.get('BASE_URL')
        token = conn_data.get('API_TOKEN')
        for ticker in tickers:
            #llamo a la API para traer data y llenar la tabla
            new_data_intraday = intraday_stock_serie(ticker, interval, base_url, token)
            new_data_sentiment = getSentiment(ticker, topics,base_url, token)
            
            #creo df
            new_data_df_intraday = pd.DataFrame.from_dict(new_data_intraday, orient='index')
            new_data_df_intraday.columns = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
            new_data_df_intraday.reset_index(inplace=True)
            new_data_df_intraday.rename(columns={'index': 'date'}, inplace=True)
            new_data_df_intraday['date'] = pd.to_datetime(new_data_df_intraday['date'])
            
            new_data_df_sentiment = pd.DataFrame(new_data_sentiment)


            # Obtener la fecha más reciente en la tabla Redshift
            print("Checkeando la fecha mas reciente en redshift")
            latest_date_query = f"SELECT MAX(date) FROM stock_intraday_prices_{ticker}"
            latest_date_in_redshift = pd.read_sql(latest_date_query, conn)["max"][0] if pd.read_sql(latest_date_query, conn)["max"][0] != None else False
            
            if(latest_date_in_redshift):
                print("latest data redshift: ",latest_date_in_redshift, "latest data API: ",new_data_df_intraday.date.max())
                #Filtrar los nuevos datos para incluir solo registros con fechas posteriores a la fecha más reciente en Redshift
                print("Filtrando la data para subir solo los updates mas recientes a redshift")
                new_data_switch = new_data_df_intraday.date.max() > latest_date_in_redshift
                mask_date = new_data_df_intraday['date']> latest_date_in_redshift
                new_data_df_intraday = new_data_df_intraday[mask_date]
                
                if (new_data_switch):
                    #nombre de las tablas
                    intraday_table_name = f'stock_intraday_prices_{ticker}'
                    sentiment_table_name = f'stock_sentiment_{ticker}'

                    # Mando df a la tabla con to_sql de pandas
                    print("nueva data:",new_data_df_intraday)

                    print("cargando data a la tabla con to_sql de pandas")
                    new_data_df_intraday.to_sql(f"{intraday_table_name}".lower(), conn,index=False,method='multi', if_exists='append')
                    new_data_df_sentiment.to_sql(f"{sentiment_table_name}".lower(), conn,index=False,method='multi', if_exists='append')
                    print("data cargada OK")
                else:
                    print("no hay nueva data")
            else:
                #nombre de las tablas
                intraday_table_name = f'stock_intraday_prices_{ticker}'
                sentiment_table_name = f'stock_sentiment_{ticker}'
                print("cargando data a la tabla con to_sql de pandas")
                new_data_df_intraday.to_sql(f"{intraday_table_name}".lower(), conn,index=False,method='multi', if_exists='append')
                new_data_df_sentiment.to_sql(f"{sentiment_table_name}".lower(), conn,index=False,method='multi', if_exists='append')
                print(f"Se cargaron {len(new_data_df_intraday)} filas en la tabla stock_prices_{ticker} y {len(new_data_df_sentiment)} en la tabla stock_sentiment_{ticker}")
            
            
dag = DAG(
    dag_id="entrega_3_dag",
    start_date=datetime(2023, 9, 11),
    schedule_interval="@daily",
    catchup=False,
)

#Advantage api configuration parameters    

tickers = ['AAPL','IBM']
interval = '60min'
topics = 'technology, manufacturing, financial_markets'

create_table_task = PythonOperator(
    task_id="create_table",
    python_callable=create_table,
    op_kwargs={
        "tickers": tickers,
        "interval": interval,
        "topics":topics,
        "engine": connect_to_db("config/config.ini")
    },
    dag=dag,
)

load_data_task = PythonOperator(
    task_id="fill_table",
    python_callable=fill_table,
    op_kwargs={
        "tickers": tickers,
        "interval": interval,
        "topics":topics,
        "engine": connect_to_db("config/config.ini")
    },
    dag=dag,
)

create_table_task >> load_data_task