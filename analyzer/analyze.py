import pandas as pd
import multiprocessing
import concurrent.futures
from constant import IS_DOCKER
import timescaledb_model as tsdb
import numpy as np
from utils import get_files_infos_df, timer_decorator, multi_read_df_from_paths
from companies import update_companies
from datetime import date
from functools import partial
from typing import Optional
import time


def init_db(
    setup=False, clean_setup=False, show_log_path=False
) -> tsdb.TimescaleStockMarketModel:
    return (
        tsdb.TimescaleStockMarketModel(
            "bourse",
            "ricou",
            "db",
            "monmdp",
            setup=setup,
            clean_setup=clean_setup,
            show_log_path=show_log_path,
        )
        if IS_DOCKER
        else tsdb.TimescaleStockMarketModel(
            "bourse",
            "ricou",
            "localhost",
            "monmdp",
            setup=setup,
            clean_setup=clean_setup,
            show_log_path=show_log_path,
        )
    )


def dfs_to_stocks2(df: pd.DataFrame) -> pd.DataFrame:
    df_stocks = df.drop(columns=["name"])
    df_stocks["volume"] = df_stocks["volume"].apply(lambda x: np.nan if x < 0 else x)
    df_stocks = df_stocks.groupby(["symbol", pd.Grouper(level=0, freq="1T")]).mean()
    df_stocks["volume"] = df_stocks["volume"].ffill()
    df_stocks = df_stocks[(df_stocks["volume"] > 0) & (df_stocks["last"] > 0)]
    df_stocks = df_stocks.reset_index(level="symbol")
    return df_stocks


def update_daystocks(
    db: tsdb.TimescaleStockMarketModel, df_stocks: pd.DataFrame, commit: bool = False
):
    df_stocks.reset_index(inplace=True)
    df_stocks["date"] = df_stocks["date"].dt.date
    df_daystocks = df_stocks.groupby(["date", "cid"]).agg(
        open=("value", "first"),
        close=("value", "last"),
        high=("value", "max"),
        low=("value", "min"),
        volume=("volume", "last"),
        mean=("value", "mean"),
        std=("value", "std"),
    )
    df_daystocks = df_daystocks.fillna(0)
    db.df_write(df_daystocks, "daystocks", commit=commit)


def update_stocks2(
    db: tsdb.TimescaleStockMarketModel,
    df: pd.DataFrame,
    symbol_to_companies: dict,
    commit: bool = False,
) -> pd.DataFrame:
    df_stocks = dfs_to_stocks2(df)
    df_stocks["cid"] = df_stocks["symbol"].map(symbol_to_companies)
    df_stocks.rename(columns={"last": "value"}, inplace=True)
    df_stocks.drop(columns=["symbol"], inplace=True)
    df_stocks["cid"] = df_stocks["cid"].astype(np.int16)
    df_stocks["value"] = df_stocks["value"].astype(np.float32)
    df_stocks["volume"] = df_stocks["volume"].astype(np.int64)
    db.df_write(df_stocks, "stocks", commit=commit)
    return df_stocks


def get_file_not_dones_df(
    db: tsdb.TimescaleStockMarketModel, files_infos_df: pd.DataFrame
) -> pd.DataFrame:
    files_names = [i[0] for i in db.raw_query("SELECT * FROM file_done")]
    return files_infos_df[~files_infos_df["name"].isin(files_names)]


def update_file_done(
    db: tsdb.TimescaleStockMarketModel,
    files_infos_df: pd.DataFrame,
    commit: bool = False,
):
    files_dones = files_infos_df["name"]
    db.df_write(files_dones, "file_done", index=False, commit=commit)


def process_date_group2(
    date_group: list[date],
    index: int,
    symbol_to_companies: dict,
    files_infos_df: pd.DataFrame,
    nb_date_group: int,
    num_thread: int,
):
    start_time = time.time()
    date_group_repr = ", ".join([d.isoformat() for d in date_group])
    index_repr = f", index:  {index} / {nb_date_group}"
    print("Processing: ", date_group_repr, index_repr)
    db = init_db()
    date_group_files_df = files_infos_df[files_infos_df["date"].isin(date_group)]
    stock_paths = list(date_group_files_df["path"])
    df = multi_read_df_from_paths(stock_paths, num_thread=num_thread)
    try:
        df_stocks = update_stocks2(db, df, symbol_to_companies)
        update_daystocks(db, df_stocks)
        update_file_done(db, date_group_files_df)
        db.commit()
    except Exception as e:
        db.connection.rollback()
        df = pd.DataFrame({"date": date_group})
        db.df_write(df, "error_dates", index=False, commit=True)
        print(date_group, "Error: ", e)
    else:
        print(
            "Done for ",
            date_group_repr,
            index_repr,
            ", time: ",
            time.time() - start_time,
            "s",
        )


def update_companies_errors(
    db: tsdb.TimescaleStockMarketModel,
    error_dates: list,
    files_infos_df: pd.DataFrame,
    num_cpus: int,
    num_thread: int,
    existing_symbols: list[str],
):
    error_dates = [e[0].date() for e in error_dates]
    files_infos_df = files_infos_df[files_infos_df["date"].isin(error_dates)]
    update_companies(
        db,
        files_infos_df,
        num_cpus,
        num_thread,
        keep_first_last=False,
        existing_symbols=existing_symbols,
    )
    cursor = db.connection.cursor()
    cursor.execute("DELETE FROM error_dates")
    db.commit()


@timer_decorator
def update_timescale_db(
    db: tsdb.TimescaleStockMarketModel,
    num_cpus: int,
    num_threads: int,
    files_infos_df: Optional[pd.DataFrame] = None,
):
    if files_infos_df is None:
        files_infos_df = get_files_infos_df()
    symbol_to_companies = {
        v: k for k, v in dict(db.raw_query("SELECT id, symbol FROM companies")).items()
    }
    if len(symbol_to_companies) == 0:
        update_companies(db, files_infos_df, num_cpus, num_threads)
        symbol_to_companies = {
            v: k
            for k, v in dict(db.raw_query("SELECT id, symbol FROM companies")).items()
        }
    files_not_dones_df = get_file_not_dones_df(db, files_infos_df)
    if len(files_not_dones_df) > 0:
        dates = files_not_dones_df["date"].unique()
        date_groups = np.array_split(dates, len(dates) // 4)
        date_groups = date_groups[:]
        indexes = np.arange(1, len(date_groups) + 1)
        process_date_group_partial = partial(
            process_date_group2,
            symbol_to_companies=symbol_to_companies,
            files_infos_df=files_not_dones_df,
            nb_date_group=len(date_groups),
            num_thread=num_threads,
        )
        # process_date_group_partial(date_groups[0], index=[0])
        with concurrent.futures.ProcessPoolExecutor(max_workers=num_cpus//2) as executor:
            executor.map(process_date_group_partial, date_groups, indexes)
    errors_dates = db.raw_query("SELECT * from error_dates")
    if len(errors_dates) > 0:
        update_companies_errors(
            db=db,
            error_dates=errors_dates,
            files_infos_df=files_infos_df,
            num_cpus=num_cpus,
            num_thread=num_threads,
            existing_symbols=[k for k, _ in symbol_to_companies.items()],
        )
        update_timescale_db(db, num_cpus, num_threads, files_infos_df)


if __name__ == "__main__":
    db = init_db(setup=True, show_log_path=True)
    num_cpus, num_threads = multiprocessing.cpu_count(), 16
    print("Start updating timescale db")
    update_timescale_db(db, num_cpus, num_threads)
    print("Finished updating timescale db")