import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s -%(levelname)s -%(message)s", force= True,
    filemode="a"
)

engine= create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    '''this function is for ETL-EXTRACT,TRANSFORM,LOAD.
    the function will load the CSV as DataFrame and ingest into a Database '''
    df.to_sql(table_name, con = engine, if_exists= 'replace' ,index=False)

def load_raw_data():
    '''This function will load the CSVs as DataFrame and ingest into db'''
    start= time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df = pd.read_csv('data/'+file)
            logging.info(f'ingesting {file} in db')
            ingest_db(df, file[:-4], engine)
        end= time.time()
        total_time=(end-start)/60
        logging.info('Ingestion Complete')
        
        logging.info(f'\nTotal time Taken: {total_time} minutes')
        
        

if __name__ == '__main__':
    load_raw_data()