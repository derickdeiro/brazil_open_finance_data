from projects.data_acquisition.shibor.extract_shibor import ExtractShibor
from projects.data_acquisition.shibor.transform_shibor import TransformShibor
from projects.data_acquisition.shibor.shibor_constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Shibor',
        'dag_description': 'Pipeline para capturar e tratar os dados de Shibor',
        'start_date': '2024-07-01',
        'schedule_interval': '0 18 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractShibor,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformShibor,
        'file_extension': '.xlsx',
        'tags': ['ITF: 10096', 'CODFAM: 125'],
    }