from projects.data_acquisition.cetip_di.extract import ExtractCetipDI
from projects.data_acquisition.cetip_di.transform import TransformCetipDI
from projects.data_acquisition.cetip_di.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Cetip_DI',
        'dag_description': 'Pipeline para capturar e tratar os dados de CETIP DI',
        'start_date': '2024-08-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractCetipDI,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformCetipDI,
        'file_extension': '.json',
        'tags': ['ITF: 100233', 'CODFAM: 118'],
    }