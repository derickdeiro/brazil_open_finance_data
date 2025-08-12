from projects.data_acquisition.anbima_fundos.extract import ExtractAnbimaFundos
from projects.data_acquisition.anbima_fundos.transform import TransformAnbimaFundos
from projects.data_acquisition.anbima_fundos.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Anbima_Fundos',
        'dag_description': 'Pipeline para capturar e tratar os dados de Fundos Anbima',
        'start_date': '2024-08-01',
        'schedule_interval': '@daily',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractAnbimaFundos,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformAnbimaFundos,
        'file_extension': '.json',
        'tags': ['anbima'],
    }