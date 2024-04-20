import pandas as pd
from constant import IS_DOCKER
import timescaledb_model as tsdb
import numpy as np
import concurrent.futures
import os
from utils import get_files_infos_df, timer_decorator
from companies import update_companies, multi_read_df_from_paths
from datetime import date
from functools import partial


def init_db() -> tsdb.TimescaleStockMarketModel:
    return (
        tsdb.TimescaleStockMarketModel("bourse", "ricou", "db", "monmdp")
        if IS_DOCKER
        else tsdb.TimescaleStockMarketModel("bourse", "ricou", "localhost", "monmdp")
    )


def dfs_to_stocks(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    df_stocks = pd.concat(dfs)
    df_stocks = df_stocks.drop(columns=["symbol", "last_suffix", "name"])
    df_stocks.reset_index(inplace=True)
    df_stocks.set_index("timestamp", inplace=True)
    start_day_index = df_stocks.between_time("09:00", "09:10").index
    df_stocks.reset_index(inplace=True)
    df_stocks.loc[~df_stocks["timestamp"].isin(start_day_index), "volume"] = (
        df_stocks.loc[~df_stocks["timestamp"].isin(start_day_index), "volume"].apply(
            lambda x: np.nan if x < 0 else x
        )
    )
    df_stocks.reset_index(inplace=True)
    df_stocks = (
        df_stocks.groupby(["symbol", pd.Grouper(key="timestamp", freq="1T")])
        .mean()
        .reset_index()
    )
    df_stocks["volume"] = df_stocks["volume"].ffill()
    df_stocks.reset_index(inplace=True)
    df_stocks = df_stocks[(df_stocks["volume"] > 0) & (df_stocks["last"] > 0)]
    if len(df_stocks[df_stocks["volume"] < 0]) > 0:
        raise ValueError("Negative volume")
    return df_stocks


def update_daystocks(db, df_stocks: pd.DataFrame):
    print("Updating daystocks")
    df_stocks.reset_index(inplace=True)
    df_stocks["date"] = df_stocks["date"].dt.date
    df_daystocks = df_stocks.groupby(["date", "cid"]).agg(
        open=("value", "first"),
        close=("value", "last"),
        high=("value", "max"),
        low=("value", "min"),
        volume=("volume", "last"),
        # mean=("value", "mean"),
        # std=("value", "std"),
    ) 
    db.copy_from_stringio(df_daystocks, "daystocks")



def update_stocks(
    db: tsdb.TimescaleStockMarketModel,
    dfs: list[pd.DataFrame],
    symbol_to_companies: dict,
) -> pd.DataFrame:
    df_stocks = dfs_to_stocks(dfs)
    df_stocks["cid"] = df_stocks["symbol"].apply(
        lambda x: symbol_to_companies.get(x, None)
    )
    df_stocks.rename(columns={"timestamp": "date", "last": "value"}, inplace=True)
    df_stocks = df_stocks[["date", "cid", "value", "volume"]]
    df_stocks.set_index("date", inplace=True)
    df_stocks["cid"] = df_stocks["cid"].astype(np.int16)
    df_stocks["value"] = df_stocks["value"].astype(np.float32)
    df_stocks["volume"] = df_stocks["volume"].astype(np.int64)
    db.copy_from_stringio(df_stocks, "stocks")
    return df_stocks


def get_file_not_dones_df(
    db: tsdb.TimescaleStockMarketModel, files_infos_df: pd.DataFrame
) -> pd.DataFrame:
    files_names = [i[0] for i in db.raw_query("SELECT * FROM file_done")]
    return files_infos_df[~files_infos_df["name"].isin(files_names)]


def update_file_done(db: tsdb.TimescaleStockMarketModel, files_infos_df: pd.DataFrame):
    files_dones = files_infos_df["name"]
    db.copy_from_stringio(files_dones, "file_done", index=False)


def process_date_group(
    date_group: list[date],
    index: int,
    symbol_to_companies: dict,
    files_infos_df: pd.DataFrame,
    nb_date_group: int,
):
    print("Processing dates group: ", date_group, "index: ", index, "/", nb_date_group)
    db = init_db()
    date_group_files_df = files_infos_df[files_infos_df["date"].isin(date_group)]
    stock_paths = list(date_group_files_df["path"])
    dfs = multi_read_df_from_paths(stock_paths)
    try:
        df_stocks = update_stocks(db, dfs, symbol_to_companies)
        update_daystocks(db, df_stocks)
    except Exception as e:
        db.connection.rollback()
        print(date_group, "Error: ", e)
    else:
        update_file_done(db, date_group_files_df)
        print("Done for ", date_group, "index: ", index, "/", nb_date_group)



@timer_decorator
def update_timescale_db(db: tsdb.TimescaleStockMarketModel):
    files_infos_df = get_files_infos_df()
    update_companies(db, files_infos_df)
    symbol_to_companies = {
        v: k for k, v in dict(db.raw_query("SELECT id, symbol FROM companies")).items()
    }
    files_not_dones_df = get_file_not_dones_df(db, files_infos_df)
    dates = files_not_dones_df["date"].unique()
    date_groups = np.array_split(dates, len(dates) // 4)
    date_groups = date_groups[:8]  # get 2 months
    indexes = np.arange(1, len(date_groups) + 1)
    process_date_group_partial = partial(
        process_date_group,
        symbol_to_companies=symbol_to_companies,
        files_infos_df=files_not_dones_df,
        nb_date_group=len(date_groups),
    )
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        executor.map(process_date_group_partial, date_groups, indexes)


if __name__ == "__main__":
    db = init_db()
    print("Start updating timescale db")
    update_timescale_db(db)
    print("Finished updating timescale db")
