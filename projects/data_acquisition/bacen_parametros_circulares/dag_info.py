from projects.data_acquisition.bacen_parametros_circulares.extract import ExtractBACEN
from projects.data_acquisition.bacen_parametros_circulares.transform import TransformBACEN
from projects.data_acquisition.bacen_parametros_circulares.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Bacen_Parametros_Circulares',
        'dag_description': 'Pipeline para capturar e tratar os dados de BACEN_PARAMETROS_CIRCULARES',
        'start_date': '2024-08-01',
        'schedule_interval': '@daily',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractBACEN,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformBACEN,
        'file_extension': '.json',
        'tags': ['ITF: 100250', 'CODFAM: 195'],
    }