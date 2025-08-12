from projects.data_acquisition.abiove_biodiesel.extract_biodiesel import ExtractBiodiesel
from projects.data_acquisition.abiove_biodiesel.transform_biodiesel import TransformBiodiesel
from projects.data_acquisition.abiove_biodiesel.constants_biodiesel import SOURCE_NAME 


main_dag = {
        'dag_name': 'Abiove_Biodiesel',
        'dag_description': 'Pipeline para capturar e tratar os dados Biodiesel da fonte ABIOVE',
        'start_date': '2024-07-01',
        'schedule_interval': '@monthly',
        'source_folder_name': SOURCE_NAME,
        'extract_function': ExtractBiodiesel,
        'blob_source_name': SOURCE_NAME,
        'transform_function': TransformBiodiesel,
        'file_extension': '.xlsx',
        'tags': ['ITF: 10227', 'CODFAM: 433'],
    }