import random
from collections import OrderedDict
from datetime import timedelta

import httpx
from airflow.decorators import task
from bs4 import BeautifulSoup


def _extract_details_from_movie_row(row):
    parts = row.split()
    standing = int(parts[0].replace(".", ""))
    title = " ".join(parts[1:-1])
    year = int(parts[-1].replace("(", "").replace(")", ""))
    return standing, title, year


def _get_movies(imdb_url):
    user_agent_list = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
    ]

    user_agent = random.choice(user_agent_list)

    headers = OrderedDict([
        ('Connection', 'keep-alive'),
        ('Accept-Encoding', 'gzip,deflate'),
        ('Origin', 'example.com'),
        ('User-Agent', user_agent),
    ])

    response = httpx.get(imdb_url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Problem with retrieving page at [{imdb_url}]")
    page = response.text
    root = BeautifulSoup(page, features="html.parser")
    result = []
    wrappers = root.find_all("tr")
    for wrapper in wrappers:
        movie_details = wrapper.find("td", {"class": "titleColumn"})
        if movie_details:
            movie_standing, movie_title, movie_year = _extract_details_from_movie_row(movie_details.text)

            movie_rating = float(wrapper.find("td", {"class": "ratingColumn"}).text)

            result.append(
                {
                    "movie_standing": movie_standing,
                    "movie_title": movie_title,
                    "movie_year": movie_year,
                    "movie_rating": movie_rating,
                }
            )

    return result


@task(task_id="get-movies", retries=10, retry_delay=timedelta(seconds=1))
def get_movies(imdb_url, xcom_result_key, **kwargs):
    ti = kwargs['task_instance']

    # get movies
    movies = _get_movies(imdb_url)
    print(movies)

    # TODO
    # to test the trend notification enable this
    # selected_movies = movies[:10]
    # random.shuffle(selected_movies)
    # for idx, movie in enumerate(selected_movies):
    #     movies[idx] = selected_movies[idx]

    # update standings
    for idx, movie in enumerate(movies):
        movies[idx]["movie_standing"] = idx

    print(movies)

    # push to xcom
    ti.xcom_push(key=xcom_result_key, value=movies)


if __name__ == '__main__':
    IMDB_URL = "https://www.imdb.com/chart/top/"
    movies = _get_movies(IMDB_URL)

    print("Done!")
