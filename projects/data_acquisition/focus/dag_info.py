from projects.data_acquisition.focus.extract import ExtractFocus
from projects.data_acquisition.focus.transform import TransformFocus
from projects.data_acquisition.focus.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Focus',
        'dag_description': 'Pipeline para capturar e tratar os dados de Focus',
        'start_date': '2024-08-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractFocus,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformFocus,
        'file_extension': '.csv',
        'tags': ['ITF: 100030', 'CODFAM: 119'],
    }