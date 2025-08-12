import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

import importlib.util
from airflow.sdk import dag  # alterado de airflow.decorators para airflow.sdk
from projects.core.data_pipeline import PipelineConstructor

data_pipeline = PipelineConstructor()

data_acquistion_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'projects', 'data_acquisition'))

for root, dirs, files in os.walk(data_acquistion_path):
    for file in files:
        if file == 'dag_info.py':
            full_path = os.path.join(root, file)
            module_name = os.path.splitext(file)[0]

            spec = importlib.util.spec_from_file_location(module_name, full_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            if hasattr(module, 'main_dag'):
                
                dag_info = module.main_dag

                SOURCE_NAME = dag_info['blob_source_name']

                dag_settings = data_pipeline.set_dag_settings(dag_name=dag_info['dag_name'],
                                                            dag_description=dag_info['dag_description'],
                                                            start_date=dag_info['start_date'],
                                                            schedule_interval=dag_info['schedule_interval'],
                                                            source_folder_name=SOURCE_NAME,
                                                            tags=dag_info['tags'])


                @dag(**dag_settings)
                def pipeline():
                    data_pipeline.etl_tasks(extract_class=dag_info['extract_function'],
                                            blob_source_name=SOURCE_NAME,
                                            transform_class=dag_info['transform_function'],
                                            # data_schema=dag_info['data_schema'],
                                            file_extension=dag_info['file_extension'])

                pipeline()
            else:
                print(f"{file}: 'main_dag' not found")