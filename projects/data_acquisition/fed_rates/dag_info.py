from projects.data_acquisition.fed_rates.extract import ExtractFEDRates
from projects.data_acquisition.fed_rates.transform import TransformFEDRates
from projects.data_acquisition.fed_rates.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'FED_Rates',
        'dag_description': 'Pipeline para capturar e tratar os dados de FED Rates',
        'start_date': '2025-02-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractFEDRates,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformFEDRates,
        'file_extension': '.xlsx',
        'tags': ['ITF: 100239', 'CODFAM: 213'],
    }