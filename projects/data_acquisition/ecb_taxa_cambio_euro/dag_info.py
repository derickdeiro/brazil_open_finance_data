from projects.data_acquisition.ecb_taxa_cambio_euro.extract import ExtractECBTaxaCambioEuro
from projects.data_acquisition.ecb_taxa_cambio_euro.transform import TransformECBTaxaCambioEuro
from projects.data_acquisition.ecb_taxa_cambio_euro.constants import SOURCE_NAME 


main_dag = {
        'dag_name': 'ECB_Taxa_Cambio_Euro',
        'dag_description': 'Pipeline para capturar e tratar os dados de Taxa de Câmbio Euro da ECB',
        'start_date': '2025-02-01',
        'schedule_interval': '0 8 * * 1-5',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractECBTaxaCambioEuro,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformECBTaxaCambioEuro,
        'file_extension': '.csv',
        'tags': ['ITF: 100076', 'CODFAM: 145'],
    }