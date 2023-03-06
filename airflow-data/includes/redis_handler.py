from datetime import (
    datetime,
    timedelta,
)

import environs
from airflow.decorators import task
from redis.client import Redis

env = environs.Env()
env.read_env()

REDIS_HOST = env.str("DATABASE_REDIS_HOST")
REDIS_PORT = env.int("DATABASE_REDIS_PORT")

DATETIME_FORMAT = '%Y-%m-%d %H-%M'


@task(task_id="store-movies-trend", retries=10, retry_delay=timedelta(seconds=1))
def save_trend(xcom_data_key, **kwargs):
    ti = kwargs['task_instance']

    redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

    movies = ti.xcom_pull(task_ids="get-movies", key=xcom_data_key)
    print(movies)

    # set movies root if not added yet
    redis_client.json().set(name="movies", path="$.trends", obj=[], nx=True)

    time_stamp = datetime.now().strftime(DATETIME_FORMAT)
    redis_client.json().arrappend(
        "movies",
        "$.trends",
        {"{}".format(time_stamp): movies}
    )


@task(task_id="update-movies-standing", retries=10, retry_delay=timedelta(seconds=1))
def update_standings(xcom_result_key, **kwargs):
    ti = kwargs['task_instance']

    redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

    # create movies_standings if not existed
    print(redis_client.json().set(name="movies", path="$.standings", obj=[], nx=True))
    print("Standings created if not existed!")
    print(redis_client.json().get("movies", "$.standings"))

    # get latest movie trend
    trends_count = redis_client.json().arrlen("movies", "$.trends")
    if trends_count is None:
        return

    trends_count = trends_count[0]

    standings_changes = []
    print(f"Total trends saved in db: {trends_count}")
    if trends_count > 0:
        # trend movie
        movies = redis_client.json().get("movies", f".trends[{trends_count - 1}]")
        print(movies)

        reference_datetime = list(movies.keys())[0]
        movies = movies[reference_datetime]
        print(movies[:10], "...")

        last_movie_standings = redis_client.json().get("movies", "$.standings")
        last_movie_standings = last_movie_standings[0]
        print("last_movie_standings:", last_movie_standings)
        print("len(last_movie_standings):", len(last_movie_standings))
        if not last_movie_standings:
            for movie in movies:
                standing = movie["movie_standing"]
                title = movie["movie_title"]
                print(standing, title)
                redis_client.json().arrappend(
                    "movies",
                    f"$.standings",
                    title
                )
                print("Added!")
                print(redis_client.json().get("movies", "$.standings"))
        else:
            for i in range(len(last_movie_standings)):
                current_stand = movies[i]
                current_title = current_stand["movie_title"]

                last_title = last_movie_standings[i]

                if last_title != current_title:
                    redis_client.json().set("movies", f"$.standings[{i}]", current_title)
                    print(f"Updated movie rank [{i + 1}]: {last_title} -> {current_title}")

                    # add to notify queue
                    standings_changes.append({
                        "standing": i,
                        "previous": last_title,
                        "current": current_title,
                    })
    print(standings_changes)
    ti.xcom_push(key=xcom_result_key, value=standings_changes)
