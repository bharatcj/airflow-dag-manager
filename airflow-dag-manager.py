import pymysql
import json
import requests
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow import DAG
from airflow.models import Variable, DagModel
from airflow.utils.state import State
from airflow import settings
from airflow.utils.email import send_email
import logging

# Get formatted date for email notifications
def get_formatted_date():
    now = datetime.now()
    day = now.day
    suffix = 'th' if 11 <= day <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')
    return now.strftime(f'%-d{suffix} %B %Y')

# Fetch DAG configurations from the database
def fetch_data_from_db():
    try:
        db_host = Variable.get("DB_HOST")
        db_name = Variable.get("DB_NAME")
        db_user = Variable.get("DB_USER")
        db_password = Variable.get("DB_PASSWORD")

        connection = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            db=db_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM airflow_scheduler 
                INNER JOIN airflow_scheduler_cstm 
                ON airflow_scheduler_cstm.id_c = airflow_scheduler.id 
                WHERE airflow_scheduler.deleted = 0
            """)
            return cursor.fetchall()

    except pymysql.MySQLError as e:
        logging.error(f"Database error: {e}")
        return []

    finally:
        if 'connection' in locals():
            connection.close()

# Send email notifications
def send_email_notification(email_to, subject, html_content, email_cc=None):
    to_list = [email.strip() for email in email_to.split(",") if email.strip()]
    cc_list = [email.strip() for email in email_cc.split(",") if email_cc and email.strip()] if email_cc else None
    
    send_email(
        to=to_list,
        cc=cc_list,
        subject=subject,
        html_content=html_content
    )

# Create an Airflow DAG dynamically
def create_dag(dag_id, schedule, default_args, select_command, api_method, email_notification, email_list, dag_name):
    
    @dag(dag_id=dag_id, schedule_interval=schedule, default_args=default_args, catchup=False)
    def dag_func():

        @task()
        def fetch_and_post_data():
            if select_command:
                try:
                    db_host = Variable.get("DB_HOST")
                    db_name = Variable.get("DB_NAME")
                    db_user = Variable.get("DB_USER")
                    db_password = Variable.get("DB_PASSWORD")

                    connection = pymysql.connect(
                        host=db_host,
                        user=db_user,
                        password=db_password,
                        db=db_name,
                        charset='utf8mb4',
                        cursorclass=pymysql.cursors.DictCursor
                    )

                    with connection.cursor() as cursor:
                        cursor.execute(select_command)
                        data = cursor.fetchall()
                        num_records = len(data)

                        item_type = "payments" if dag_id == "autopay_payment_process" else "records"

                        # Send email notification for DAG start
                        if email_notification:
                            send_email_notification(
                                email_list,
                                f"DAG {dag_name} Started",
                                f"<b>{dag_name}</b> Job Started on {get_formatted_date()} for {num_records} {item_type}."
                            )

                except pymysql.MySQLError as e:
                    logging.error(f"Database error: {e}")

                    # Notify about failure
                    if email_notification:
                        send_email_notification(
                            email_list,
                            f"DAG {dag_name} Failed",
                            f"The DAG {dag_name} encountered an error: {e}"
                        )
                    return

                finally:
                    if 'connection' in locals():
                        connection.close()

                # Process data and send to API
                api_url = Variable.get("API_URL")
                bearer_token = Variable.get("BEARER_TOKEN")

                for row in data:
                    for key, value in row.items():
                        if isinstance(value, datetime):
                            row[key] = value.isoformat()

                    payload = {
                        "data": [row],
                        "method": api_method
                    }

                    headers = {
                        'Content-Type': 'application/json',
                        'Authorization': f'Bearer {bearer_token}'
                    }

                    response = requests.post(api_url, headers=headers, data=json.dumps(payload))

                    if response.status_code == 200:
                        logging.info(f"Data {row} posted successfully")
                    else:
                        logging.error(f"Failed to post data {row}: {response.status_code} - {response.text}")

                # Send completion email
                if email_notification:
                    send_email_notification(
                        email_list,
                        f"DAG {dag_name} Completed",
                        f"All {num_records} {item_type} have been successfully processed."
                    )

        fetch_and_post_data()

    return dag_func()

# Manage DAG pause state
def manage_dag_pause_state(dag_id, is_paused):
    session = settings.Session()
    dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
    
    if dag_model:
        dag_model.is_paused = is_paused
        session.commit()

# Fetch DAG configurations and generate DAGs dynamically
dag_configs = fetch_data_from_db()

for dag_config in dag_configs:
    dag_id = dag_config['dag_id']
    schedule = dag_config['schedule']
    start_date = dag_config['start_date'].strftime("%Y-%m-%d")
    retries = dag_config.get('retries', 1)
    retry_delay = dag_config.get('retry_delay', 5)
    email_notification = dag_config.get('email_notification_c', 0)
    email_list = dag_config.get('email', '')
    is_paused = dag_config['is_paused_c'] == 1
    select_command = dag_config['select_command_c']
    api_method = dag_config['api_method_c']

    default_args = {
        "owner": dag_config['owner'],
        "start_date": start_date,
        "retries": retries,
        "retry_delay": timedelta(minutes=retry_delay),
        "email_on_failure": dag_config.get('email_on_failure', 0),
        "email_on_retry": dag_config.get('email_on_retry', 0),
        "email": email_list
    }

    globals()[dag_id] = create_dag(dag_id, schedule, default_args, select_command, api_method, email_notification, email_list, dag_id)
    
    manage_dag_pause_state(dag_id, is_paused)
