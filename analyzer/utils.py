from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import pandas as pd
from tqdm import tqdm
import pandas as pd
import os
import time
from constant import DATA_PATH, DATA_PATH_SAMY, FILES_INFO_PATH
from models import FileInfo


def get_files_infos_windows_df(cache = False) -> pd.DataFrame:
    def _get_files_infos_df():
        files_infos = []

        for root, dirs, files in os.walk(DATA_PATH_SAMY):
            if len(dirs) > 0:
                continue
            year_str = os.path.basename(root)
            try:
                year = int(year_str)
            except ValueError:
                continue
            for file in files:
                if file[0] == ".":
                    continue
                market = file.split(" ")[0]
                timestamp = " ".join(file.split(" ")[1:]).split(".")[0]
                files_infos.append(
                    {
                        "market": market,
                        "path": os.path.join(root, file),
                        "year": year,
                        "timestamp": timestamp,
                    }
                )
        files_infos_df = pd.DataFrame(files_infos)
        files_infos_df["timestamp"] = pd.to_datetime(
            files_infos_df["timestamp"], format="%Y-%m-%d %H_%M_%S"
        )
        files_infos_df["timestamp"] = pd.to_datetime(files_infos_df["timestamp"])
        files_infos_df.set_index("timestamp", inplace=True)
        files_infos_df.sort_index(inplace=True)
        files_infos_df["year_month"] = files_infos_df.index.to_period("M")  # type: ignore
        files_infos_df["date"] = files_infos_df.index.date  # type: ignore
        files_infos_df["name"] = files_infos_df["path"].apply(lambda x: os.path.basename(x))
        files_infos_df.to_pickle(FILES_INFO_PATH)
        return files_infos_df
    if cache: 
        try:
            return pd.read_pickle(FILES_INFO_PATH)
        except:
            return _get_files_infos_df()
    else:
        return _get_files_infos_df()

def get_files_infos_df(cache = False) -> pd.DataFrame:
    def _get_files_infos_df():
        files_infos: list[FileInfo] = []
        for root, dirs, files in os.walk(DATA_PATH):
            if len(dirs) > 0:
                continue
            year = int(root.split("/")[-1])
            for file in files:
                if file[0] == ".":
                    continue
                market = file.split(" ")[0]
                timestamp = " ".join(file.split(" ")[1:]).split(".")[0]
                files_infos.append(
                    FileInfo(
                        market=market,
                        path=os.path.join(root, file),
                        year=year,
                        timestamp=timestamp,
                    )
                )
        files_infos_df = pd.DataFrame(files_infos)
        files_infos_df["timestamp"] = pd.to_datetime(files_infos_df["timestamp"])
        files_infos_df.set_index("timestamp", inplace=True)
        files_infos_df.sort_index(inplace=True)
        files_infos_df["year_month"] = files_infos_df.index.to_period("M")  # type: ignore
        files_infos_df["date"] = files_infos_df.index.date  # type: ignore
        files_infos_df["name"] = files_infos_df["path"].apply(lambda x: os.path.basename(x))
        files_infos_df.to_pickle(FILES_INFO_PATH)
        return files_infos_df
    if cache:
        try:
            return pd.read_pickle(FILES_INFO_PATH)
        except:
            return _get_files_infos_df()
    else:
        return _get_files_infos_df()


def read_file(path: str) -> pd.DataFrame:
    df: pd.DataFrame = pd.read_pickle(path)
    df["last_suffix"] = df["last"].astype(str).str.extract(r"\((.*?)\)", expand=False)
    df["last"] = (
        df["last"]
        .astype(str)
        .str.replace(r"\((.*?)\)", "", regex=True)
        .str.replace(" ", "")
        .astype(float)
    )
    timestamp = " ".join(path.split(" ")[1:]).split(".")[0]
    df["timestamp"] = pd.to_datetime(timestamp)
    df["symbol"] = df["symbol"].astype(str)
    df["name"] = df["name"].astype(str)
    df.attrs = {"timestamp": timestamp}
    return df


def multi_read_df_from_paths(
    paths: list[str], read_function=read_file
) -> list[pd.DataFrame]:
    dfs = []
    num_cpus = multiprocessing.cpu_count()
    with ThreadPoolExecutor(max_workers=num_cpus) as executor:
        futures = []
        for path in paths:
            futures.append(executor.submit(read_function, path))
        for future in tqdm(futures):
            dfs.append(future.result())
    return dfs


def timer_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Function {func.__name__} took {(end_time - start_time)} seconds to run.")
        return result
    return wrapper