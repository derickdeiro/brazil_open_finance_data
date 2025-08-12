from projects.data_acquisition.bndes_moedas_contratuais.extract import ExtractMoedasContratuais
from projects.data_acquisition.bndes_moedas_contratuais.transform import TransformMoedasContratuais
from projects.data_acquisition.bndes_moedas_contratuais.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'BNDES_Moedas_Contratuais',
        'dag_description': 'Pipeline para capturar e tratar os dados de Moedas Contratuais da fonte BNDES',
        'start_date': '2024-12-09',
        'schedule_interval': '@daily',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractMoedasContratuais,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformMoedasContratuais,
        'file_extension': '.txt',
        'tags': ['ITF: 90011345', 'CODFAM: 137'],
    }