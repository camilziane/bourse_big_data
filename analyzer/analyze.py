import pandas as pd
from constant import IS_DOCKER
import timescaledb_model as tsdb
import numpy as np
import concurrent.futures
import os
from utils import get_files_infos_df
from companies import update_companies, multi_read_df_from_paths
from datetime import datetime, timedelta, date

from tqdm import tqdm


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
    df_stocks = df_stocks[(df_stocks["volume"] > 0 ) & (df_stocks["last"] > 0)]
    print(len(df_stocks[df_stocks["volume"] < 0]))
    return df_stocks


def update_daystocks(db, dates_group: list[date]):
    db.execute(
        f"""
  INSERT INTO daystocks (date, cid, open, close, high, low, volume)
  SELECT
  DATE(date) AS date,
  cid,
  FIRST(value, date) AS open,
  LAST(value, date) AS close,
  MAX(value) AS high,
  MIN(value) AS low,
  LAST(volume, date) AS volume
  FROM
  stocks
  WHERE
  date > '{dates_group[0]}' and date < '{dates_group[1] +timedelta(days=1) if len(dates_group) > 1 else dates_group[0] + timedelta(days=1)}'
  GROUP BY
    cid,
    DATE(date)
  ORDER BY
    DATE(date) DESC
  """,
        commit=True,
    )


def update_stocks(
    db: tsdb.TimescaleStockMarketModel,
    dfs: list[pd.DataFrame],
    symbol_to_companies: dict,
):
    df_stocks = dfs_to_stocks(dfs)
    df_stocks["cid"] = df_stocks["symbol"].apply(
        lambda x: symbol_to_companies.get(x, None)
    )
    df_stocks.rename(columns={"timestamp": "date", "last": "value"}, inplace=True)
    df_stocks = df_stocks[["date", "cid", "value", "volume"]]
    df_stocks.set_index("date", inplace=True)
    df_stocks["cid"] = df_stocks['cid'].astype(np.int16)
    df_stocks['value'] = df_stocks['value'].astype(np.float32)
    df_stocks['volume'] = df_stocks['volume'].astype(np.int64)
    db.copy_from_stringio(df_stocks, "stocks")


def get_file_not_dones_df(
    db: tsdb.TimescaleStockMarketModel, files_infos_df: pd.DataFrame
) -> pd.DataFrame:
    files_names = [i[0] for i in db.raw_query("SELECT * FROM file_done")]
    return files_infos_df[~files_infos_df["name"].isin(files_names)]


def update_file_done(db: tsdb.TimescaleStockMarketModel, files_infos_df: pd.DataFrame):
    files_dones = files_infos_df["name"]
    db.df_write(files_dones, "file_done", index=False)


def update_timescale_db(db: tsdb.TimescaleStockMarketModel):
    companies = db.raw_query("SELECT * FROM companies")
    files_infos_df = get_files_infos_df()

    if len(companies) == 0:
        companies_files = files_infos_df.groupby(["market", "date"]).first()
        companies_files.reset_index(inplace=True)
        dfs_amsterdam = multi_read_df_from_paths(
            list(companies_files[companies_files["market"] == "amsterdam"]["path"])
        )
        dfs_compA = multi_read_df_from_paths(
            list(companies_files[companies_files["market"] == "compA"]["path"])
        )
        dfs_compB = multi_read_df_from_paths(
            list(companies_files[companies_files["market"] == "compB"]["path"])
        )
        dfs_peapme = multi_read_df_from_paths(
            list(companies_files[companies_files["market"] == "peapme"]["path"])
        )
        update_companies(db, dfs_amsterdam, dfs_compA, dfs_compB, dfs_peapme)
        symbol_to_companies = {
            v: k
            for k, v in dict(db.raw_query("SELECT id, symbol FROM companies")).items()
        }
    symbol_to_companies = {
        v: k for k, v in dict(db.raw_query("SELECT id, symbol FROM companies")).items()
    }
    files_not_dones_df = get_file_not_dones_df(db, files_infos_df)
    dates = files_not_dones_df["date"].unique()
    dates = np.array_split(dates, len(dates) // 1)
    dates = dates[:2]

    def process_dates_group(dates_group):
        print("Processing dates group: ", dates_group)
        files_infos_df_group = files_infos_df[files_infos_df["date"].isin(dates_group)]
        stock_files = files_infos_df_group["path"]
        dfs = multi_read_df_from_paths(list(stock_files))
        try:
            update_stocks(db, dfs, symbol_to_companies)
            # update_daystocks(db, dates_group)
        except Exception as e:
            db.connection.rollback()
            print(e)
            print("Error, for ", dates_group)
        else:
            update_file_done(db, files_infos_df_group)
            print("Done for ", dates_group)

    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        list(tqdm(executor.map(process_dates_group, dates), total=len(dates)))


if __name__ == "__main__":
    print(IS_DOCKER)
    db = (
        tsdb.TimescaleStockMarketModel("bourse", "ricou", "db", "monmdp")
        if IS_DOCKER
        else tsdb.TimescaleStockMarketModel("bourse", "ricou", "localhost", "monmdp")
    )
    update_timescale_db(db)
    print("Done")
