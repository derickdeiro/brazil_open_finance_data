from projects.data_acquisition.anbima_indices_ida.extract import ExtractAnbimaIDA
from projects.data_acquisition.anbima_indices_ida.transform import TransformAnbimaIDA
from projects.data_acquisition.anbima_indices_ida.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Anbima_IDA',
        'dag_description': 'Pipeline para capturar e tratar os dados de Índices de preços de IDA da ANBIMA',
        'start_date': '2025-04-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractAnbimaIDA,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformAnbimaIDA,
        'file_extension': '.json',
        'tags': ['ITF: 10042', 'CODFAM: 185'],
    }