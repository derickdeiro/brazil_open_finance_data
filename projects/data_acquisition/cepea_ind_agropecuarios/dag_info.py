from projects.data_acquisition.cepea_ind_agropecuarios.extract import ExtractCepeaIndAgropecuarios
from projects.data_acquisition.cepea_ind_agropecuarios.transform import TransformCepeaIndAgropecuarios
from projects.data_acquisition.cepea_ind_agropecuarios.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Cepea_ind_agropecuarios',
        'dag_description': 'Pipeline para capturar e tratar os dados de CEPEA indices agropecuarios',
        'start_date': '2024-08-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractCepeaIndAgropecuarios,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformCepeaIndAgropecuarios,
        'file_extension': '.csv',
        'tags': ['ITF: 100247', 'CODFAM: 146'],
    }