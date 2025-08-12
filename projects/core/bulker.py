from azure.identity import DefaultAzureCredential

import pandas as pd
from bcpandas import to_sql, SqlCreds
from typing import List, Optional, Union
import pyodbc, struct
import uuid

# from airflow.sdk import Connection

class Bulker:
    def __init__(self, database: str = "cvm"):
        """
        Initializes the Bulker with the database connection string.
        Args:
            database (str): The name of the database to connect to.
        """

        self._creds = SqlCreds(
            server="bdsdev.database.windows.net",
            database=database,
            username="infra",
            password="bdssolucoes123!@",
            driver_version=18,
        )
        connection_string = (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=bdsdev.database.windows.net;"
            f"Database={database};"
            "Uid=infra;"
            "Pwd=bdssolucoes123!@;"
        )

        # credential = DefaultAzureCredential( exclude_interactive_browser_credential=True)
        # token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
        # token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
        # SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h
        self._conn = pyodbc.connect(connection_string, )# attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
        self._table_id = uuid.uuid4().hex

    @property
    def table_id(self) -> str:
        return self._table_id
    
    def __enter__(self):
        if not self._conn:
            raise ValueError("Connection is not established.")
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self._conn:
            self._conn.close()
            self._conn = None

    def _execute_merge(self, table_name:str, key_collumn:Union[str, List[str]], all_collumn:List[str], delete_condition: Optional[str] = None ) -> bool:
        if isinstance(key_collumn, str):
            key_collumn = [key_collumn]
        
        for key in key_collumn:
            if key not in all_collumn:
                raise ValueError(f"Key column '{key}' must be in the list of all columns.")
            else:
                all_collumn.remove(key)

        update_part = ', '.join([f"target.[{col}] = source.[{col}]" for col in all_collumn])
        insert_fields_part = ', '.join([f"[{col}]" for col in (key_collumn + all_collumn)])
        insert_values_part = ', '.join([f"source.[{col}]" for col in (key_collumn + all_collumn)])
        update_condition = ' OR '.join([f"target.[{col}] <> source.[{col}] OR (target.[{col}] IS NULL AND source.[{col}] IS NOT NULL)" for col in all_collumn])
        
        match_condition = ' AND '.join([f"target.[{key}] = source.[{key}]" for key in key_collumn])

        query_init = f"""
        MERGE INTO [{table_name}] AS target
        USING ##{table_name}_{self.table_id} AS source
        ON {match_condition}
        """
        query_update = f"""
        WHEN MATCHED AND ({update_condition}) THEN
            UPDATE SET {update_part}
        """
        query_insert = f"""
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_fields_part})
            VALUES ({insert_values_part})
        """

        if delete_condition:
            query_delete_merge = f"""
            WHEN NOT MATCHED BY SOURCE AND {delete_condition} THEN
                DELETE;
            """
        else:
            query_delete_merge = f"""
            WHEN NOT MATCHED BY SOURCE THEN
                DELETE;
            """

        query_delete = f"""
        DROP TABLE ##{table_name}_{self.table_id};
        """
        query = query_init
        
        if update_part:
            query += query_update
        
        if insert_fields_part:
            query += query_insert

        if query_delete_merge:
            query += query_delete_merge
            
        query += query_delete
        
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            self._conn.commit()
            return True
        except pyodbc.Error as e:
            print(f"Error merging data into {table_name}: {e}")
            print(f"Query: {query}")
            return False
    
    def process_data(self, table_name: str, key_collumn:Union[str, List[str]], df: pd.DataFrame, delete_condition: Optional[str] = None) -> bool:
        """
        Processes the data by uploading it to a temporary table and merging it into the target table.

        Args:
            table_name (str): The name of the target table.
            key_collumn (Union[str, List[str]]): The key column(s) to match on.
            df (pd.DataFrame): The DataFrame containing the data to be processed.
            delete_condition (Optional[str]): Optional condition for deleting rows in the target table.
        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        to_sql(df=df, table_name=f"##{table_name}_{self.table_id}", creds=self._creds, if_exists="fail", index=False, schema="dbo", collation="SQL_Latin1_General_CP1_CS_AS")
        return self._execute_merge(table_name, key_collumn, df.columns.to_list())