from projects.data_acquisition.anbima_indices_idka.extract import ExtractAnbimaIDkA
from projects.data_acquisition.anbima_indices_idka.transform import TransformAnbimaIDkA
from projects.data_acquisition.anbima_indices_idka.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Anbima_IDkA',
        'dag_description': 'Pipeline para capturar e tratar os dados de Índices de preços de IDkA da ANBIMA',
        'start_date': '2025-04-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractAnbimaIDkA,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformAnbimaIDkA,
        'file_extension': '.json',
        'tags': ['ITF: 10042', 'CODFAM: 185'],
    }