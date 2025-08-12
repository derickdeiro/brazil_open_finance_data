from projects.data_acquisition.anbima_debentures.extract import ExtractAnbimaDebentures
from projects.data_acquisition.anbima_debentures.transform import TransformAnbimaDebentures
from projects.data_acquisition.anbima_debentures.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Anbima_Debentures',
        'dag_description': 'Pipeline para capturar e tratar os dados de Mercado Secundário de Debentures da ANBIMA',
        'start_date': '2025-04-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractAnbimaDebentures,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformAnbimaDebentures,
        'file_extension': '.xls',
        'tags': ['ITF: 100101', 'CODFAM: 171'],
    }