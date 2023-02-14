from airflow.utils.email import send_email
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

def failure_callback(context):
    """
    The function that will be executed on DAG failure.

    :param context: The context of the executed task.
    :type context: dict
    """

    context['task_instance'].xcom_push(key="error_code", value=context['task_instance'].dag_id)
    context['task_instance'].xcom_push(key="process_status", value="fail")

    try:
        context['task_instance'].xcom_push(key="error_desc", value=context['exception'])
    except Exception as ex:
        context['task_instance'].xcom_push(key="error_desc", value="Unable to convert exception details, see execution log for details: {log_url}".format(log_url=context['task_instance'].log_url))

    dagid = context['task_instance'].dag_id
    taskid = context['task_instance'].task_id
    failreason = str(context['exception'])
    execdate = context['execution_date']
    logurl = context['task_instance'].log_url
           
    message = ':ampelred: AIRFLOW TASK FAILURE :ampelred:\n' \
              '\n' \
              'DAG:                             {dag}\n' \
              'TASKS:                          {task}\n' \
              'EXECUTION TIME:      {exec_date}\n'  \
              'REASON:                       {reason}\n' \
              'LOG:                              {log_url}\n' \
        .format(dag=dagid,
                task=taskid,
                reason=failreason,
                exec_date=execdate,
                log_url=logurl)
    print(message)


    blocks = [
        {
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": ":exclamation:Airflow Failure:exclamation:",
				"emoji": True
			}
		},
		{
			"type": "divider"
		},
        {
			"type": "section",
			"fields": [
				{
					"type": "mrkdwn",
					"text": "*DAG*"
				},
				{
					"type": "mrkdwn",
					"text": f"{dagid}"
				},
				{
					"type": "mrkdwn",
					"text": "*Task*"
				},
				{
					"type": "mrkdwn",
					"text": f"{taskid}"
				},
				{
					"type": "mrkdwn",
					"text": "*Reason*"
				},
				{
					"type": "mrkdwn",
					"text": f"{failreason}"
				},
				{
					"type": "mrkdwn",
					"text": "*Log*"
				},
				{
					"type": "mrkdwn",
					"text": f"<{logurl}|View Log>"
				}
			]
		},
		# {
		# 	"type": "section",
		# 	"text": {
		# 		"type": "mrkdwn",
		# 		"text": f"*DAG*\t\t\t\t{dagid}\n*Task*\t\t\t\t{taskid}\n*Reason*\t\t\t{failreason}\n*Log*\t\t\t\t<{logurl}|View Log>"
		# 	}
		# },
		{
			"type": "divider"
		},
		{
			"type": "context",
			"elements": [
				{
					"type": "mrkdwn",
					"text": "To assign this failure to someone, create a Git issue."
				}
			]
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": f"<https://github.groupondev.com/FSA/JLA/issues/new?assignees=&labels=bug&template=jla-bug-report.md&title=Airflow+%7C+{dagid}+{failreason[:40]}|:octocat::git: Create Issue>"
			}
		}
    ]

    slack_conn = context['task_instance'].xcom_pull(key="slack_conn_id")
    slack_webhook_token = context['task_instance'].xcom_pull(key="slack_webhook_token")
    slack_channel = context['task_instance'].xcom_pull(key="slack_channel")
    
    print(slack_conn)
    print(slack_webhook_token)
    print(slack_channel)
    
    failed_alert = SlackWebhookOperator(
        task_id='slack_fail_alert',
        http_conn_id=slack_conn,
        webhook_token=slack_webhook_token,
        #message=message,
        blocks=blocks,
        channel=slack_channel,
        username='airflow',
        dag=context['dag'])

    failed_alert.execute(context=context)
    
    return


def success_callback(context):
    """
    The function that will be executed on DAG success.

    :param context: The context of the executed task.
    :type context: dict
    """
    context['task_instance'].xcom_push(key="process_status", value="success")
    subject = f"Daily Merchant Payments Reconciliation ETL | {context['execution_date']} | Success"
    message = 'Airflow Details:\n' \
              'DAG:     {}\n' \
              'DAG Run: {}\n' \
        .format(context['task_instance'].dag_id,
                context['dag_run'].run_id)
    print(f"Subject: {subject} \nMessage: {message}")

    # send notification
    send_email(context['email'], subject, message)
    
    return