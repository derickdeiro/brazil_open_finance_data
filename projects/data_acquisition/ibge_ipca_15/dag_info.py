from projects.data_acquisition.ibge_ipca_15.extract import ExtractIPCA15
from projects.data_acquisition.ibge_ipca_15.transform import TransformIPCA15
from projects.data_acquisition.ibge_ipca_15.constants import SOURCE_NAME 

main_dag = {
        'dag_name': 'IBGE_IPCA_15',
        'dag_description': 'Pipeline para capturar e tratar os dados de IPCA_15',
        'start_date': '2024-08-01',
        'schedule_interval': '@monthly',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractIPCA15,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformIPCA15,
        'file_extension': '.xlsx',
        'tags': ['ITF: 100058', 'CODFAM: 131'],
    }