import botocore
from airflow.models import Variable
import boto3
import psycopg2
import logging
from google.oauth2 import service_account
import json
from sqlalchemy import create_engine

class CredentialManagerFromVariables:

    def __init__(self,
                 aws_config='aws_bigd_config',
                 pg_config='pg_bigd_config',
                 google_config='google_aviator_config',):
        '''
        Main manager of creds
        :param aws_config: Change aws airflow json
        if you want different aws config by default 'aws_bigd_config'
        :param pg_config: Change pg airflow json
        if you want different postgres config by default 'pg_bigd_config'
        '''
        self._aws_session = None
        self._pg_conn = None
        self._pg_engine = None

        # get from airflow variables
        self.aws_json_config = Variable.get(aws_config, deserialize_json=True)
        self.AWS_ACCESS_KEY_ID = self.aws_json_config['AWS_ACCESS_KEY_ID']
        self.AWS_SECRET_ACCESS_KEY = self.aws_json_config['AWS_SECRET_ACCESS_KEY']
        self.AWS_SESSION_TOKEN = self.aws_json_config.get('AWS_SESSION_TOKEN')
        self.AWS_DEFAULT_REGION = self.aws_json_config['AWS_DEFAULT_REGION']

        self.pg_json_config = Variable.get(pg_config, deserialize_json=True)
        self.postgres_db_name = self.pg_json_config['dbname']
        self.postgres_host = self.pg_json_config['host']
        self.postgres_port = self.pg_json_config['port']
        self.postgres_user = self.pg_json_config['username']
        self.postgres_password = self.pg_json_config['password']

        self.google_json_config = Variable.get(google_config)

    def get_aws_session(self):
        '''
        Get AWS session
        :return: _aws_session
        '''
        if self._aws_session is None:
            try:
                self._aws_session = boto3.Session(
                    aws_access_key_id=self.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
                    aws_session_token=self.AWS_SESSION_TOKEN,
                    region_name=self.AWS_DEFAULT_REGION)
                logging.info('Successfully connected to AWS:')
            except Exception as e:
                logging.error(e)
        return self._aws_session


    def get_google_session(self):
        cred_google_json = json.loads(self.google_json_config)
        credentials = service_account.Credentials.from_service_account_info(
            cred_google_json,
        )
        return credentials

### Postgres
    def get_pg_session(self):
        '''
        Get PG session
        :return:
        '''
        if self._pg_conn is None:
            try:
                logging.info('Trying to connect to pg...')

                self._pg_conn = psycopg2.connect(
                    host=self.postgres_host,
                    port=self.postgres_port,
                    database=self.postgres_db_name,
                    user=self.postgres_user,
                    password=self.postgres_password,
                )
                print(f'Successfully connected to pg, db: {self.postgres_db_name}')
            except Exception as e:
                logging.error(e)
        return self._pg_conn


    def get_pg_engine(self):
        '''
        This method return pg engine
        and it used for df.to_sql() pd function
        :return:
        '''
        if self._pg_engine is None:
            url = (
                f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db_name}"
            )
            self._pg_engine = create_engine(url)

        return self._pg_engine

    def get_pg_adbc_uri(self) -> str:
        """
        For ABDC driver for polars
        postgresql://user:password@host:port/dbname
        """
        user = self.pg_json_config['username']
        password = self.pg_json_config['password']
        host = self.pg_json_config['host']
        port = self.pg_json_config['port']
        dbname = self.pg_json_config['dbname']

        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


class CredentialManagerFromConnection:
    pass

class CredentialManagerFromSecretAWS:
    pass
# cm = CredentialManager()

# cm.check_pg_connection()