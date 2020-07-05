import pandas as pd
import numpy as np
import pymongo
import json
from logger import logger

LOGGER = logger('upload')


with open('assets/creds.json') as f:
    CREDS = json.load(f)


def insert_data(file, db, collection, creds, replace=False):
    """
    Insert data from file to db mongodb's collection
    By default data is appened to collection.
    Parameters
    ----------
    file: str
        Path to file with data
    db: str
        Name of the database to insert to
    collection: str
        Name of the collection to insert to
    cred: str
        Credentials to connect to db with, connection string
    replace: bool
        Whether to drop collection before insert

    Returns
    -------
    status: bool
        Insert operation results, True if successful
    """
    LOGGER.info(f'Initiating insert data from {file} to {db}:{collection}')
    myclient = pymongo.MongoClient(creds)
    mydb = myclient[db]
    collections = mydb.list_collection_names()
    
    if replace and collection in collections:
        LOGGER.info(f'Dropping collection {collection}')
        drop_col = mydb[collection]
        drop_col.drop()
    
    col = mydb[collection]
    data = pd.read_csv(file)
    data = data.replace({np.nan: None})
    # converting string to datetime
    
    LOGGER.info(f'Inserting data of shape {data.shape} to {collection}')
    recs = data.reset_index(drop=True).to_dict('records')

    ids = col.insert_many(recs)

    status = len(ids.inserted_ids) == data.shape[0]
    LOGGER.info(f'Inserted to {collection} successfully: {status}')

    return status


def run_all():
    LOGGER.info('Running all jobs initiated')
    files = [
        'data/orders_202002181303.csv',
        'data/users_202002181303.csv',
    ]
    collections = [
        'orders',
        'users'
    ]
    creds = CREDS['mongodb_kesha']
    status = True
    for file, collection in zip(files, collections):
        LOGGER.info(f'Running insert job for colleciton: {collection}')
        ok = insert_data(file, 'kesha', collection, creds,replace=True)
        status &= ok

    LOGGER.info(f'All jobs finished successfully: {status}')

    return status


if __name__ == "__main__":
    run_all()
