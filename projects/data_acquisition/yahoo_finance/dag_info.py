from projects.data_acquisition.yahoo_finance.extract import ExtractYahooFinance
from projects.data_acquisition.yahoo_finance.transform import TransformYahooFinance
from projects.data_acquisition.yahoo_finance.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Yahoo_Finance',
        'dag_description': 'Pipeline para capturar e tratar os dados de Yahoo Finance',
        'start_date': '2024-08-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractYahooFinance,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformYahooFinance,
        'file_extension': '.csv',
        'tags': ['ITF: 100064', 'CODFAM: 132'],
    }