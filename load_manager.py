from de_framework.cred_manager import CredentialManagerFromVariables
from de_framework.data_validation import DfDataValidation
from de_framework.progress_logger import ProgressLogger

import awswrangler as wr
import sys
import logging
import pandas as pd
import os
from airflow.models import Variable
from airflow.sdk import get_current_context
from pathlib import Path
import numpy as np
from airflow.exceptions import AirflowFailException
from psycopg2.extras import execute_values
import os
from botocore.config import Config




class DownloadLoadManager:
    def __init__(self,
                 aws_config='aws_bigd_config',
                 pg_config='pg_bigd_config',
                 google_config='google_aviator_config'):
        self.cm = CredentialManagerFromVariables(
            aws_config=aws_config,
            pg_config=pg_config,
            google_config=google_config,
        )
        self.s3_athena_output = 's3://big-d-data/athena_results/agg_adtarget_pipeline/'

        # local path from airflow
        self.default_path = Variable.get('AIRFLOW_LOCAL_DATA')

        self.dv = DfDataValidation()


    def save_results_locally(self, **kwargs):
        '''
        df: awdda \n
        path: name of pipeline folder
        df_file_name:
        fmt: format csv, parquet, json
        :param header:
        :param prefix:
        df pandas dataframe
        path folder in airwlod_data
        df_file_name filename
        fmt format of file
        header need or no
        :return: save dataframe
        '''
    # get values from kwargs
        df = kwargs.get("df")
        path = kwargs.get("path")
        df_file_name = kwargs.get("df_file_name")
        fmt = kwargs.get("format", "csv")
        header = kwargs.get("header", True)
        prefix = kwargs.get("prefix", '')


    # create dir if it not exists
        full_dir = f'{self.default_path}/{path}'
        os.makedirs(full_dir, exist_ok=True)
        full_path = f'{full_dir}/{df_file_name}_{prefix}.{fmt}'

    # save results locally
        if fmt == 'csv':
            df.to_csv(full_path, index=False, header=header)
        elif fmt == 'parquet':
            df.to_parquet(full_path, index=False)
        elif fmt == 'json':
            df.to_json(full_path, orient='records', lines=True)
        else:
            raise ValueError(f'Unsupported format: {fmt}')

        print(f'💾 Saved to {full_path}')
        return full_path

    def get_sql_file(self, sql_file, sql_dir):
        '''

        :param sql_file: file in folder that will be executed
        :param context:
        :return:
        '''
        # get formatted date and timestamp from logical date
        ctx = get_current_context()
        ds_formatted = ctx['logical_date'].strftime('%Y-%m-%d')
        ts_formatted = ctx['logical_date'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        print('Date Variables that will be used:',ds_formatted, ts_formatted, sep='\n')

        sql = (Path(sql_dir) / f'{sql_file}.sql').read_text()
        return (sql
                .replace('{ds_formatted}', ds_formatted)
                .replace('{ts_formatted}', ts_formatted))

        # variables will be used in file in format
        sql = (sql
               .replace('{ds_formatted}', ds_formatted)
               .replace('{ts_formatted}', ts_formatted))

        return sql

    def download_from_athena_to_df(self,
                             path,
                             df_file_name,
                             sql_file,
                             sql_dir,
                             header=True,
                            prefix='',
                             format="csv",
                             athena_db='bebra',
                             s3_athena_output=None,
                            **context

    ):
        '''
        This function downloads data from athena to pandas dataframe
        :param path: folder in airflow_data
        :param df_file_name: how to name the dataframe
        :param sql_file: sql file that will be executed
        :param header: by default True
        :param format: by default Csv
        :param athena_db: by default bebra
        :param s3_athena_output: folder where athena result will be saved
        :return: dataframe
        '''

        session = self.cm.get_aws_session()
        wr.config.botocore_config = Config(
            read_timeout=3600,
            connect_timeout=60,
            retries={"max_attempts": 5, "mode": "adaptive"},
        )

        if s3_athena_output is None:
            s3_athena_output = self.s3_athena_output



        # Comment this if you have user creds  and can delete cte#
        wr.engine.set("python")
        wr.memory_format.set("pandas")

        sql_query_text = self.get_sql_file(sql_file, sql_dir)
        # Athena query run
        query_id = wr.athena.start_query_execution(
            sql=sql_query_text,
            database=athena_db,
            boto3_session=session,
            s3_output=s3_athena_output
        )
        print('===== EXECUTING SQL =====')
        sys.stdout.write(sql_query_text + '\n')
        print('===== END SQL =====')
        print("Query ID Running:", query_id)

        # Get dataframe
        df = wr.athena.get_query_results(query_execution_id=query_id,
                                         boto3_session=session)



        print(f"Query id:{query_id}, Scanned data amount:"
              f"{round(df.query_metadata['Statistics']['DataScannedInBytes'] / 1024 ** 3, 5)} GB")
        logging.info(df.query_metadata['Statistics'])

        # save df locally with default
        self.save_results_locally(df=df,
                                  path=path,
                                  df_file_name=df_file_name,
                                  format=format,
                                  prefix=prefix,
                                  header=header)

        return df

    def load_from_df_to_postgres(self,
                                 df,
                                 pg_table_name,
                                 on_conflict=False,
                                 conflict_columns=None,  # список колонок-ключа, напр. ['zendesk_ticket_id','etl_dt']
                                 update_columns=None, # какие обновлять при конфликте; None = все кроме ключа
                                 ):
        '''
        Get dataframe from folder and insert into postgres \n
        Automatically add prefix ts_formatted
        :param df: dataframe file
        :param pg_table_name: like marts.adtarget where to insert data into
        :return:
        '''

        df_tuples = list(df.itertuples(index=False, name=None))
        columns = list(df.columns)

        if not df_tuples:
            print(f'Nothing to insert into {pg_table_name}')
            return

        # get session from cred_manager
        pg_session = self.cm.get_pg_session()

        # Get fancy progress bar
        progress = ProgressLogger(
            total=len(df_tuples),
            desc=f'Inserting into {pg_table_name}'
        )
        # --- Get SQL ---
        base = f"INSERT INTO {pg_table_name} ({', '.join(columns)}) VALUES %s"

        if on_conflict:
            if not conflict_columns:
                raise AirflowFailException("on_conflict=True требует conflict_columns")

            # по умолчанию обновляем все колонки, КРОМЕ ключа конфликта
            if update_columns is None:
                update_columns = [c for c in columns if c not in conflict_columns]

            set_clause = ', '.join(f"{c} = EXCLUDED.{c}" for c in update_columns)
            sql = (f"{base} "
                   f"ON CONFLICT ({', '.join(conflict_columns)}) "
                   f"DO UPDATE SET {set_clause}")
        else:
            sql = base

        # --- одна общая ветка вставки (без дублирования кода) ---
        try:
            batch_size = 2000
            with pg_session.cursor() as cur:
                for i in range(0, len(df_tuples), batch_size):
                    batch = df_tuples[i:i + batch_size]
                    execute_values(cur, sql, batch, page_size=batch_size)
                    progress.update(len(batch))
            pg_session.commit()
            progress.finish()
        except Exception as e:
            progress.fail(e)
            pg_session.rollback()
            raise AirflowFailException(f'Failed: {e}')

    # def load_from_bq_to_(self,)

    # def merge_from_df_to_postgres(self,
    #                               df,
    #                               pg_table_name,
    #                               matching_column=None,
    #                               columns_to_update=None,
    #                               columns_to_insert=None,):
    #     pg_session = self.cm.get_pg_session()
    #
    #     df_tuples = list(df.itertuples(index=False, name=None))
    #
    #     # Get fancy progress bar
    #     progress = ProgressLogger(
    #         total=len(df_tuples),
    #         desc=f'Merging df into {pg_table_name}'
    #     )
    #     try:
    #
    #         sql = (f'MERGE INTO {pg_table_name} AS target '
    #                f'USING source_table AS source '
    #                f'ON target.{matching_column} = source.{matching_column}'
    #                f'WHEN MATCHED THEN'
    #                f'UPDATE SET target.column1 = source.column1,'
    #                f'        target.column2 = source.column2'
    #                f'WHEN NOT MATCHED THEN'
    #                f'        INSERT (column1, column2, column3)'
    #                f'        VALUES (source.column1, source.column2, source.column3)'
    #                f'WHEN NOT MATCHED BY SOURCE THEN DELETE;')
    #
    #     except Exception as e:
    #         # fail bar
    #         progress.fail(e)
    #
    #         # rollback session
    #         pg_session.rollback()
    #         raise AirflowFailException(f'Failed: {e}')


