from projects.data_acquisition.us_treasury.extract import ExtractUSTreasury
from projects.data_acquisition.us_treasury.transform import TransformUSTreasury
from projects.data_acquisition.us_treasury.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'us_treasury',
        'dag_description': 'Pipeline para capturar e tratar os dados de us_treasury',
        'start_date': '2024-08-01',
        'schedule_interval': '0 8 * * 1-5',

        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractUSTreasury,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformUSTreasury,
        'file_extension': '.csv',
        'tags': ['ITF: 100082', 'CODFAM: 125'],
    }