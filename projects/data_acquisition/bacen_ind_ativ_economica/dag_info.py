from projects.data_acquisition.bacen_ind_ativ_economica.extract import ExtractIndAtivEconomica
from projects.data_acquisition.bacen_ind_ativ_economica.transform import TransformIndAtivEconomica
from projects.data_acquisition.bacen_ind_ativ_economica.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'Bacen_Ind_Ativ_Economica',
        'dag_description': 'Pipeline para capturar e tratar os dados Índice de Atividade Econômica da fonte BACEN',
        'start_date': '2024-07-01',
        'schedule_interval': '@monthly',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractIndAtivEconomica,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformIndAtivEconomica,
        'file_extension': '.json',
        'tags': ['ITF: 10211', 'CODFAM: 216'],
    }