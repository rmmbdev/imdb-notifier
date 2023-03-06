from datetime import timedelta

import pika
from airflow.decorators import task

credentials = pika.PlainCredentials('guest', 'guest')
connection_params = pika.ConnectionParameters('rabbitmq', 5672, '/', credentials)

QUEUE_NAME = "notifications"


@task(task_id="send-notification-to-queue", retries=10, retry_delay=timedelta(seconds=1))
def send_notifications_to_queue(xcom_data_key, **kwargs):
    ti = kwargs['task_instance']

    # get changed movies
    standings_changes = ti.xcom_pull(task_ids="update-movies-standing", key=xcom_data_key)
    print(standings_changes)

    # create connections
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    for movie in standings_changes:
        message = f"standing [{movie['standing'] + 1}]: {movie['previous']} -> {movie['current']}"
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=bytes(message, 'utf-8')
        )
        print(message)

    connection.close()
