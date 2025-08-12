from projects.data_acquisition.scot.extract import ExtractScot
from projects.data_acquisition.scot.transform import TransformScot
from projects.data_acquisition.scot.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Scot',
        'dag_description': 'Pipeline para capturar e tratar os dados de boi gordo da fonte Scot',
        'start_date': '2024-12-10',
        'schedule_interval': '* 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractScot,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformScot,
        'file_extension': '.json',
        'tags': ['ITF: 10095', 'CODFAM: 290'],
    }