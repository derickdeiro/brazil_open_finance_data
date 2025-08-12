from projects.data_acquisition.cetip_taxa_de_juros.extract import ExtractCetip
from projects.data_acquisition.cetip_taxa_de_juros.transform import TransformCetip
from projects.data_acquisition.cetip_taxa_de_juros.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Cetip_Taxa_de_Juros',
        'dag_description': 'Pipeline para capturar e tratar os dados de CETIP Taxas de Juros',
        'start_date': '2024-08-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractCetip,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformCetip,
        'file_extension': '.json',
        'tags': ['ITF: 100028', 'CODFAM: 118'],
    }