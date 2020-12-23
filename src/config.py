from os import getenv as env
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


class BaseConfig:
    """
    Holds environment variables from the .env
    """
    DATA_FOLDER = env("BASE_DATA_PATH")
    MOVIE_LENS_FOLDERS = env("MOVIE_LENS")
    FRIENDS_DATASET = env("FRIENDS")
