from projects.data_acquisition.anbima_indices_ihfa.extract import ExtractAnbimaIHFA
from projects.data_acquisition.anbima_indices_ihfa.transform import TransformAnbimaIHFA
from projects.data_acquisition.anbima_indices_ihfa.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Anbima_IHFA',
        'dag_description': 'Pipeline para capturar e tratar os dados de Índices de preços de IHFA da ANBIMA',
        'start_date': '2025-04-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractAnbimaIHFA,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformAnbimaIHFA,
        'file_extension': '.json',
        'tags': ['ITF: 10042', 'CODFAM: 185'],
    }