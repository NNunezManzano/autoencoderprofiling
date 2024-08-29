import requests
import os
import logging
import json
from dotenv import load_dotenv


#TODO: Comentar
class AlphaConnect:

    def __init__(self, apikey,check=True):
        self.apikey = apikey
        self.url = 'https://www.alphavantage.co'
        function='NEWS_SENTIMENT'
        uri = f'{self.url}/query?function={function}&apikey={self.apikey}'
        # Setup logging
        logging.basicConfig(filename='alpha_connect.log', level=logging.ERROR,
                            format='%(asctime)s:%(levelname)s:%(message)s')
        if check:
            self.__check_connection(uri)
    
    def getFundamental(self, symbol:str, BalanceSheet:bool = True, IncomeStatement:bool = False, CashFlow:bool = False)-> dict:
        dataDict = {'Ticker':symbol}
        if BalanceSheet:
            balance_sheet_url = f'{self.url}/query?function=BALANCE_SHEET&symbol={symbol}&apikey={self.apikey}'
            try:
                balance_sheet_response = requests.get(balance_sheet_url)
            except requests.exceptions.RequestException as e:
                logging.error("Failed to get balance sheet", exc_info=True)
                print(e)
            if not balance_sheet_response == None:
                balance_sheet_data = balance_sheet_response.json()
                dataDict['BalanceSheetData'] = balance_sheet_data

        if IncomeStatement:            
            income_statement_url = f'{self.url}/query?function=INCOME_STATEMENT&symbol={symbol}&apikey={self.apikey}'
            try:
                income_statement_response = requests.get(income_statement_url)
            except requests.exceptions.RequestException as e:
                logging.error("Failed to get balance sheet", exc_info=True)
                print(e)
            if not income_statement_response == None:
                income_statement_data = income_statement_response.json()
                dataDict['IncomeStatementData'] = income_statement_data

        if CashFlow:
            cash_flow_url = f'{self.url}/query?function=CASH_FLOW&symbol={symbol}&apikey={self.apikey}'
            try:
                cash_flow_response = requests.get(cash_flow_url)
            except requests.exceptions.RequestException as e:
                logging.error("Failed to get balance sheet", exc_info=True)
                print(e)
            if not cash_flow_response == None:
                cash_flow_data = cash_flow_response.json()
                dataDict['CashFlowData'] = cash_flow_data

        return dataDict
    
    def __check_connection(self, uri):
        try:
            response = requests.get(uri)
            if response.status_code == 200:
                print(f"Successfully connected to Alpha Vantage")
            else:
                print(f"Failed to connect to Alpha Vantage. Status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error("Failed to connect to Alpha Vantage", exc_info=True)
            print(f"Failed to connect to Alpha Vantage: {e}")

if __name__ == '__main__':
    # El símbolo de la empresa (MSFT para Microsoft)
    symbol = 'MSFT'
    load_dotenv()
    apikey = os.getenv('API_KEY_9')

    alphaclient = AlphaConnect(apikey)

    data = alphaclient.getFundamental(symbol)

    with open('alphaData.json','w') as dataJson:
        json.dump(data, dataJson, indent=4)




