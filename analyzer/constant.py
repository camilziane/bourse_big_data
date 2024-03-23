import os

DATA_PATH = os.getenv('DATA_PATH', "../data")
IS_DOCKER = os.getenv('IS_DOCKER', "False") == "True"
