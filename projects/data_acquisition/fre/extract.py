import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.fre.constants import SOURCE_NAME

class ExtractFRE(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
      
      pass