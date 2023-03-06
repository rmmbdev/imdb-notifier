from datetime import (
    datetime,
    timedelta,
)

from airflow import DAG
from includes.imdb_handler import get_movies
from includes.rabbit_handler import send_notifications_to_queue
from includes.redis_handler import (
    save_trend,
    update_standings,
)

with DAG(
    dag_id='top_movies_notifier',
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['imdb', 'crawl', 'notify'],
) as dag:
    get_movies(
        imdb_url="https://www.imdb.com/chart/top/",
        xcom_result_key="imdb_movies",
    ) >> save_trend(
        xcom_data_key="imdb_movies",
    ) >> update_standings(
        xcom_result_key="standing_changes",
    ) >> send_notifications_to_queue(
        xcom_data_key="standing_changes",
    )
