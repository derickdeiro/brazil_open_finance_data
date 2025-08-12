from projects.data_acquisition.bcb_ptax.extract import ExtractBCBPtax
from projects.data_acquisition.bcb_ptax.transform import TransformBCBPtax
from projects.data_acquisition.bcb_ptax.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'BCB_Ptax',
        'dag_description': 'Pipeline para capturar e tratar os dados de Taxa Ptax do Banco Central do Brasil',
        'start_date': '2025-02-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractBCBPtax,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformBCBPtax,
        'file_extension': '.csv',
        'tags': ['ITF: 100136', 'CODFAM: 193'],
    }