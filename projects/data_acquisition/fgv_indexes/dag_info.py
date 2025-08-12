from projects.data_acquisition.fgv_indexes.extract import ExtractFGVIndex
from projects.data_acquisition.fgv_indexes.transform import TransformFGVIndex
from projects.data_acquisition.fgv_indexes.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'FGV_Indices',
        'dag_description': 'Pipeline para capturar e tratar os dados de FGV Indices DI, Mercado e 10',
        'start_date': '2025-06-10',
        'schedule_interval': '0 8 10 * *',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractFGVIndex,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformFGVIndex,
        'file_extension': '.xls',
        'tags': ['ITF: 10020', 'CODFAM: 114'],
    }