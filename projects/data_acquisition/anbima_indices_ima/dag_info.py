from projects.data_acquisition.anbima_indices_ima.extract import ExtractAnbimaIMA
from projects.data_acquisition.anbima_indices_ima.transform import TransformAnbimaIMA
from projects.data_acquisition.anbima_indices_ima.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Anbima_IMA',
        'dag_description': 'Pipeline para capturar e tratar os dados de Índices de preços de IMA da ANBIMA',
        'start_date': '2025-04-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractAnbimaIMA,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformAnbimaIMA,
        'file_extension': '.json',
        'tags': ['ITF: 10042', 'CODFAM: 185'],
    }