import os
DATA_PATH = os.getenv('DATA_PATH', os.path.join("..", "data"))
IS_DOCKER = os.getenv('IS_DOCKER', "False") == "True"
DATA_PATH_SAMY = os.getenv('DATA_PATH', r"C:\Users\Samy\Desktop\pythonBigData\project\bourse_big_data\data")
FILES_INFO_PATH = os.path.join(DATA_PATH, 'files_infos.pkl')

