from projects.data_acquisition.bcb_pu550.extract import ExtractPU550
from projects.data_acquisition.bcb_pu550.transform import TransformPU550
from projects.data_acquisition.bcb_pu550.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'BCB_PU550',
        'dag_description': 'Pipeline para capturar e tratar os dados de BCB PU550',
        'start_date': '2025-04-01',
        'schedule_interval': '@daily',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractPU550,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformPU550,
        'file_extension': '.txt',
        'tags': ['ITF: 100157', 'CODFAM: 206'],
    }