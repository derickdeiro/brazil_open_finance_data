import os
import sys
from bs4 import BeautifulSoup
from io import BytesIO

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.yahoo_finance.constants import SOURCE_NAME, TICKERS
import yfinance as yf
from io import BytesIO

class ExtractYahooFinance(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):

        for ticker in TICKERS:
            temp_data = yf.download(tickers=f'^{ticker}', period='5d', interval='1d', auto_adjust=True, actions=True)
           
            raw_data = BytesIO()
            temp_data.to_csv(raw_data, index=True)
            raw_data.seek(0)
           
            if not temp_data.empty:     
                self.upload_raw_data(source_name=SOURCE_NAME, raw_data=raw_data, dataser=exec_date, url_or_file_name=f'{ticker}.csv')            
            else: 
                raise f'Erro ao realizar a captura do arquivo f{ticker}.csv.'
 