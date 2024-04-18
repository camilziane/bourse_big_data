import pandas as pd
import numpy as np
from utils import multi_read_df_from_paths, get_files_infos_df
from timescaledb_model import TimescaleStockMarketModel


map_prefix_to_symbol_nf = {
    "1rP": lambda x: x[3:].split("_")[0] if len(x) != 15 else np.nan,  # EuroNext Pars
    "1rA": lambda x: x[3:],  # EuroNext Amsterdam
    "1rE": lambda x: x[4:],  # EuroNext Paris
    "FF1": lambda x: x.split("_")[1][0:],  # EuroNext Brussels
}


def update_ticker_column(
    df: pd.DataFrame,
) -> pd.DataFrame:
    df["ticker"] = df.apply(
        lambda x: map_prefix_to_symbol_nf.get(x["prefix"], lambda _: x["symbol"])(
            x["symbol"]
        ),
        axis=1,
    )
    return df


def update_mid_column(
    df: pd.DataFrame,
    prefix_to_market_id: dict,
    default_mid: int,
) -> pd.DataFrame:
    df["mid"] = df.apply(
        lambda x: prefix_to_market_id.get(x["prefix"], default_mid),
        axis=1,
    )
    return df


def dfs_to_companie(
    dfs: list[pd.DataFrame],
    prefix_to_market_id: dict,
    default_mid: int,
    is_pea: bool = False,
) -> pd.DataFrame:
    df_all_days = pd.concat(dfs)
    df_all_days_grouped = (
        df_all_days.reset_index(drop=True).groupby(["symbol", "name"]).last()
    )
    df_all_days_grouped["name"] = df_all_days_grouped.index.get_level_values(1)
    df_companies = df_all_days_grouped.groupby(
        df_all_days_grouped.index.get_level_values(0)
    ).last()
    df_companies = df_companies.reset_index()[["symbol", "name", "timestamp"]]
    df_companies["prefix"] = df_companies["symbol"].apply(lambda x: x[0:3])
    df_companies = update_ticker_column(df_companies)
    df_companies = update_mid_column(
        df_companies, prefix_to_market_id, default_mid=default_mid
    )
    df_companies = df_companies.drop(columns=["prefix"])
    df_companies["pea"] = is_pea
    return df_companies


def update_companies(
    db: TimescaleStockMarketModel,
    dfs_amsterdam: list[pd.DataFrame],
    dfs_compA: list[pd.DataFrame],
    dfs_compB: list[pd.DataFrame],
    dfs_peapme: list[pd.DataFrame],
):
    amsterdam_companies = dfs_to_companie(
        dfs_amsterdam, db.prefix_to_market_id, default_mid=db.nasdaq_market_id
    )

    compA_companies = dfs_to_companie(
        dfs_compA,
        prefix_to_market_id=db.prefix_to_market_id,
        default_mid=db.prefix_to_market_id["1rP"],
    )

    compB_companies = dfs_to_companie(
        dfs_compB,
        prefix_to_market_id=db.prefix_to_market_id,
        default_mid=db.prefix_to_market_id["1rP"],
    )

    peapme_companies = dfs_to_companie(
        dfs_peapme,
        prefix_to_market_id=db.prefix_to_market_id,
        default_mid=db.prefix_to_market_id["1rP"],
        is_pea=True,
    )
    df_companies = [
        amsterdam_companies,
        compA_companies,
        compB_companies,
        peapme_companies,
    ]
    df_companies = pd.concat(df_companies)
    df_companies.sort_values(by="timestamp", inplace=True)
    df_companies.drop_duplicates(inplace=True)
    df_companies.drop_duplicates(subset=["symbol"], keep="last", inplace=True)
    df_companies.drop("timestamp", axis=1, inplace=True)
    df_companies.set_index("symbol", inplace=True)
    db.df_write(df_companies, "companies", commit=True)
