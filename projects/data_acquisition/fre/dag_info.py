from projects.data_acquisition.fre.extract import ExtractFRE
from projects.data_acquisition.fre.transform import TransformFRE
from projects.data_acquisition.fre.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'FRE',
        'dag_description': 'Pipeline para capturar e tratar os dados de FRE',
        'start_date': '2024-08-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractFRE,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformFRE,
        'file_extension': '.xml',
        'tags': ['fre', 'finance'],
    }