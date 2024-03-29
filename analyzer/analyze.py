import pandas as pd
import os
from constant import DATA_PATH, IS_DOCKER, DATA_PATH_SAMY
import timescaledb_model as tsdb
from models import FileInfo
from typing import Optional


def get_files_infos_windows_df(backup_path: Optional[str] = None) -> pd.DataFrame:
    if backup_path:
        return pd.read_pickle(backup_path)
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
    return files_infos_df


def get_files_infos_df(backup_path: Optional[str] = None) -> pd.DataFrame:
    if backup_path:
        return pd.read_pickle(backup_path)
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
    return files_infos_df


# TODO
def read_file(path: str) -> pd.DataFrame:
    df: pd.DataFrame = pd.read_pickle(path)
    df["last_suffix"] = df["last"].str.extract(r"\((.*?)\)", expand=False)
    df["last"] = (
        df["last"]
        .astype(str)
        .str.replace(r"\((.*?)\)", "", regex=True)
        .str.replace(" ", "")
        .astype(float)
    )
    df.attrs = {"timestamp": " ".join(path.split(" ")[1:]).split(".")[0]}
    return df


if __name__ == "__main__":
    print(IS_DOCKER)
    db = (
        tsdb.TimescaleStockMarketModel("bourse", "ricou", "db", "monmdp")
        if IS_DOCKER
        else tsdb.TimescaleStockMarketModel("bourse", "ricou", "localhost", "monmdp")
    )
    print("Done")
