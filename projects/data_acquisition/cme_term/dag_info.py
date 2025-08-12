from projects.data_acquisition.cme_term.extract import ExtractCMETerm
from projects.data_acquisition.cme_term.transform import TransformCMETerm
from projects.data_acquisition.cme_term.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'CME_Term',
        'dag_description': 'Pipeline para capturar e tratar os dados de CME Term',
        'start_date': '2025-02-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractCMETerm,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformCMETerm,
        'file_extension': '.json',
        'tags': ['ITF: 100165', 'CODFAM: 213'],
    }