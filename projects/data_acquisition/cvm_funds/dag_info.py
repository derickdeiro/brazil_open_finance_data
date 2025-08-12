from projects.data_acquisition.cvm_funds.extract import ExtractCVMFunds
from projects.data_acquisition.cvm_funds.transform import TransformCVMFunds
from projects.data_acquisition.cvm_funds.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'CVM_Fundos',
        'dag_description': 'Pipeline para capturar e tratar os dados de CME Term',
        'start_date': '2025-02-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractCVMFunds,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformCVMFunds,
        'file_extension': '.csv',
        'tags': ['ITF: 10235', 'CODFAM: 450'],
    }