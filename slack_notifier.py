from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from airflow.models import Variable


SLACK_TOKEN = Variable.get('SLACK_API_TOKEN')
ENVIRONMENT = Variable.get('ENVIRONMENT')
AIRFLOW_SERVER_LOCATION = Variable.get('AIRFLOW_SERVER_LOCATION')

CHANNEL_ID = "#airflow_alerts"
CHANNEL_ID_DBT = "#dbt_alerts"
base_url = AIRFLOW_SERVER_LOCATION

def slack_notification_on_fail(context):
    '''
    This function represents failure each time when pipeline was not ok
    :param context:
    :return: alert into slack channel
    '''
    if ENVIRONMENT == 'prod':
        client = WebClient(token=SLACK_TOKEN)
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        execution_date = context['ts']
        try:
            response = client.chat_postMessage(
                channel=CHANNEL_ID,
                text=f':x: Dag failed: {dag_id} \n'
                     f'{task_id}--{execution_date}\n\n'
                     f'Link for failed dag: \n'
                     f'{base_url}/{dag_id}'
            )
            print("Message sent:", response["ts"])

        except SlackApiError as e:
            print(f"Error: {e.response['error']}")
    else:
        print('Slack notification turned off, change airflow ENVIRONMENT to prod')


def slack_notification_on_success(context):
    '''
    This function represents success each time when pipeline was ok
    :param context:
    :return: alert into slack channel
    '''
    if ENVIRONMENT == 'prod':
        client = WebClient(token=SLACK_TOKEN)
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        execution_date = context['ts']
        try:
            response = client.chat_postMessage(
                channel=CHANNEL_ID,
                text=f':white_check_mark: Dag success: {dag_id} \n'
                     f'{task_id}--{execution_date} \n\n'
                     f'Link for successed dag: \n'
                     f'{base_url}/{dag_id}'

            )
            print("Message sent:", response["ts"])

        except SlackApiError as e:
            print(f"Error: {e.response['error']}")
    else:
        print('Slack notification turned off, change airflow ENVIRONMENT to prod')


def slack_dbt_test_warn(context):

    '''
    This function represents warnings of dbt test
    :param context: airflow context
    :return: alert into slack channel
    '''
    if ENVIRONMENT == 'prod':

        test_names = context.get('test_names')
        test_results = context.get('test_results')

        if not test_names:  # freshness / ложные срабатывания Cosmos
            return

        client = WebClient(token=SLACK_TOKEN)

        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        execution_date = context['ts']

        details = "\n".join(
            f"• *{name}*\n```{result}```"
            for name, result in zip(test_names, test_results)
        )


        try:
            response = client.chat_postMessage(
                channel=CHANNEL_ID_DBT,
                text=f':warning: dbt test warnings: {dag_id}\n'
                     f'{task_id}--{execution_date}\n\n'
                     f'{details}\n\n'
                     f'Link: \n{base_url}/{dag_id}'
            )
            print("Message sent:", response["ts"])

        except SlackApiError as e:
            print(f"Error: {e.response['error']}")
    else:
        print('Slack notification turned off, change airflow ENVIRONMENT to prod')
