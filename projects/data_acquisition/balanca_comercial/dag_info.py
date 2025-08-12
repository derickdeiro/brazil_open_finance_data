from projects.data_acquisition.balanca_comercial.extract_balanca_comercial import ExtractBalancaComercial
from projects.data_acquisition.balanca_comercial.transform_balanca_comercial import TransformBalancaComercial
from projects.data_acquisition.balanca_comercial.constants_balanca_comercial import SOURCE_NAME 


main_dag = {
        'dag_name': 'Balanca_Comercial',
        'dag_description': 'Pipeline para capturar e tratar os dados de Balança Comercial',
        'start_date': '2024-07-01',
        'schedule_interval': '@monthly',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractBalancaComercial,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformBalancaComercial,
        'file_extension': '.xlsx',
        'tags': ['ITF: 10278', 'CODFAM: 495'],
    }