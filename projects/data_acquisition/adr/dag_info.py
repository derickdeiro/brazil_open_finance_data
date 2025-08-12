from projects.data_acquisition.adr.extract_adr import ExtractADR
from projects.data_acquisition.adr.transform_adr import TransformADR
from projects.data_acquisition.adr.adr_constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'ADR',
        'dag_description': 'Pipeline para capturar e tratar os dados de ADR',
        'start_date': '2024-08-01',
        'schedule_interval': '@monthly',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractADR,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformADR,
        'file_extension': '.xlsx',
        'tags': ['ITF: 10205', 'CODFAM: 414'],
    }