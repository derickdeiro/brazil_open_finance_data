from projects.data_acquisition.fipe_ipc.extract import ExtractFipeIPC
from projects.data_acquisition.fipe_ipc.transform import TransformFipeIPC
from projects.data_acquisition.fipe_ipc.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Fipe_IPC',
        'dag_description': 'Pipeline para capturar e tratar os dados de IPC da Fipe',
        'start_date': '2025-04-01',
        'schedule_interval': '@monthly',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractFipeIPC,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformFipeIPC,
        'file_extension': '.json',
        'tags': ['ITF: 10069', 'CODFAM: 111'],
    }