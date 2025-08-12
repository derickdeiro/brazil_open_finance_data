from projects.data_acquisition.cepea_cotacoes_commodities.extract import ExtractCEPEA
from projects.data_acquisition.cepea_cotacoes_commodities.transform import TransformCEPEA
from projects.data_acquisition.cepea_cotacoes_commodities.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Cepea_Cotacoes_Commodities',
        'dag_description': 'Pipeline para capturar e tratar os dados de CEPEA - Cotacoes de Commodities',
        'start_date': '2024-08-01',
        'schedule_interval': '@daily',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractCEPEA,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformCEPEA,
        'file_extension': '.xlsx',
        'tags': ['ITF: 100248', 'CODFAM: 218'],
    }