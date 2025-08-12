from projects.data_acquisition.balanca_comercial_epei.extract_epei import ExtractBalancaComlEPEI
from projects.data_acquisition.balanca_comercial_epei.transform_epei import TransformBalancaComlEPEI
from projects.data_acquisition.balanca_comercial_epei.constants_epei import SOURCE_NAME 


main_dag = {
        'dag_name': 'Balanca_Comercial_EPEI',
        'dag_description': 'Pipeline para capturar e tratar os dados de Balança Comercial EPEI',
        'start_date': '2024-07-01',
        'schedule_interval': '@monthly',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractBalancaComlEPEI,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformBalancaComlEPEI,
        'file_extension': '.xlsx',
        'tags': ['ITF: 10276', 'CODFAM: 493'],
    }