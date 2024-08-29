from pymongo.mongo_client import MongoClient
import requests
import pandas as pd
import os
from dotenv import load_dotenv
import getData as gd

import mongoConnection as mc
import logging

import json

#TODO: Comentar
logging.basicConfig(filename='main_run_error.log', level=logging.ERROR,
                            format='%(asctime)s:%(levelname)s:%(message)s')


def addData(mongoClient:MongoClient, data, ticker):
    #TODO:  Agregar logueo de aca en adelante
    mgdb = mongoClient.dbConnect('Fundamental')
    if not mgdb == None:
        for key, values in data.items():
            if key == 'BalanceSheetData':
                    collection = mongoClient.collectionConnect(mgdb, "BalanceSheetData")
                    if not collection == None:
                        inserted_id = mongoClient.insertDoc(collection, values)
                        print(f"Inserted document ID: {inserted_id} | {ticker} | {key}")

            if key == 'IncomeStatementData':
                    collection = mongoClient.collectionConnect(mgdb, "IncomeStatementData")
                    if not collection == None:
                        inserted_id = mongoClient.insertDoc(collection, values)
                        print(f"Inserted document ID: {inserted_id} | {ticker} | {key}")

            if key == 'CashFlowData':
                    collection = mongoClient.collectionConnect(mgdb, "CashFlowData")
                    if not collection == None:
                        inserted_id = mongoClient.insertDoc(collection, values)
                        print(f"Inserted document ID: {inserted_id} | {ticker} | {key}")
    else:
         logging.warning(f"MongoClient {mgdb}")
         print(f"MongoClient {mgdb}")

def getSymbols(mongoClient:MongoClient,BalanceSheet:bool = False, IncomeStatement:bool = False, CashFlow:bool = False):
     
    mgClient_db = mongoClient.dbConnect('Fundamental')
     
    if BalanceSheet:
        collection = mongoClient.collectionConnect(mgClient_db, "BalanceSheetData")
    elif IncomeStatement:
        collection = mongoClient.collectionConnect(mgClient_db, "IncomeStatementData")
    elif CashFlow:
        collection = mongoClient.collectionConnect(mgClient_db, "CashFlowData")
    else:
         logging.warning(f"Collection not found")
         print("Collection not found")
         return []
    
    all_documents = collection.find()
    docDict = {}
    
    for doc in all_documents:
        docDict[doc['_id']] = doc
    
    symbolLS = []
    
    for id, doc in docDict.items():
        try:
            symbolLS.append(doc['symbol'])
        except KeyError:
            logging.error(f"'symbol' key not found for record id {id}", exc_info=True)
            print(f"{id}")

    return symbolLS

if __name__ == '__main__':
    
    load_dotenv()

    #credenciales mongo
    mgUser = os.getenv('MONGO_USER')
    mgPass = os.getenv('MONGO_KEY')
    mgCluster = os.getenv('MONGO_CLUSTER')
    appName = os.getenv('MONGO_APP')

    #credenciales alpha vantage
    apikey = os.getenv('API_KEY_1')

    BalanceSheet    = False
    IncomeStatement = False
    CashFlow        = True

    tickers_df = pd.read_csv('MVPs/Large.csv')
    tickers = set(tickers_df.Ticker.values)
    
    alphaClient = gd.AlphaConnect(apikey,check=False)
    mgClient = mc.Mongoconnect(mgUser, mgPass, mgCluster, appName)

    apiCount = 0    
    creditsApikey = True
    symbolLS = getSymbols(mgClient, BalanceSheet, IncomeStatement, CashFlow)

    for ticker in tickers:
    
        if ticker in symbolLS:
            print(ticker)
            continue

        if creditsApikey:
            data = alphaClient.getFundamental(ticker, BalanceSheet, IncomeStatement, CashFlow)
            addData(mgClient, data, ticker=ticker)
            apiCount += 1
           
            if apiCount == 25:
                creditsApikey = False
    
    print(apiCount)
             
 
