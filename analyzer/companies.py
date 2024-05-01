import pandas as pd
import concurrent.futures
import numpy as np
from utils import  multi_read_df_from_paths, timer_decorator
from timescaledb_model import TimescaleStockMarketModel
from functools import partial


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


def df_to_companie(
    df: pd.DataFrame,
    prefix_to_market_id: dict,
    default_mid: int,
    is_pea: bool = False,
) -> pd.DataFrame:
    df.reset_index(inplace=True)
    df_all_days_grouped = df.groupby(["symbol", "name"]).last()
    df_all_days_grouped["name"] = df_all_days_grouped.index.get_level_values(1)
    df_companies = df_all_days_grouped.groupby(
        df_all_days_grouped.index.get_level_values(0)
    ).last()
    df_companies = df_companies.reset_index()[["symbol", "name", "date"]]
    df_companies["prefix"] = df_companies["symbol"].apply(lambda x: x[0:3])
    df_companies = update_ticker_column(df_companies)
    df_companies = update_mid_column(
        df_companies, prefix_to_market_id, default_mid=default_mid
    )
    df_companies = df_companies.drop(columns=["prefix"])
    df_companies["pea"] = is_pea
    return df_companies


def files_to_companies(
    companies_files_paths: list[str],
    default_mid: int,
    prefix_to_market_id: dict,
    num_thread: int,
):
    df = multi_read_df_from_paths(companies_files_paths, num_thread=num_thread)
    companies = df_to_companie(
        df,
        prefix_to_market_id=prefix_to_market_id,
        default_mid=default_mid,
    )
    return companies


@timer_decorator
def update_companies(
    db: TimescaleStockMarketModel,
    files_infos_df: pd.DataFrame,
    num_cpus: int,
    num_thread: int,
    keep_first_last=True,
    existing_symbols=[],
):
    print("Updating companies...")
    if keep_first_last:
        companies_files_first = files_infos_df.groupby(["market", "date"]).first()
        companies_files_last = files_infos_df.groupby(["market", "date"]).last()
        companies_files = pd.concat([companies_files_first, companies_files_last])
        companies_files.reset_index(inplace=True)
    else:
        companies_files = files_infos_df
        companies_files.reset_index(inplace=True)
    companies_files_paths = [
        list(companies_files[companies_files["market"] == "amsterdam"]["path"]),
        list(companies_files[companies_files["market"] == "compA"]["path"]),
        list(companies_files[companies_files["market"] == "compB"]["path"]),
        list(companies_files[companies_files["market"] == "peapme"]["path"]),
    ]
    companies_default_mid = [
        db.eurex_market_id,
        db.prefix_to_market_id["1rP"],
        db.prefix_to_market_id["1rP"],
        db.prefix_to_market_id["1rP"],
    ]
    files_to_companies_partial = partial(
        files_to_companies,
        prefix_to_market_id=db.prefix_to_market_id,
        num_thread=num_thread,
    )
    companies_default_mid = [
        companies_default_mid[i]
        for i, c in enumerate(companies_files_paths)
        if len(c) > 0
    ]
    companies_files_paths = [c for c in companies_files_paths if len(c) > 0]
    companies_list = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cpus) as executor:
        companies_list = [
            r
            for r in executor.map(
                files_to_companies_partial, companies_files_paths, companies_default_mid
            )
        ]
    df_companies = pd.concat(companies_list)
    df_companies = df_companies[~df_companies["symbol"].isin(existing_symbols)]
    update_comanies_to_db(db, df_companies)


def update_comanies_to_db(db: TimescaleStockMarketModel, df_companies: pd.DataFrame):
    df_companies.sort_values(by="date", inplace=True)
    df_companies.drop_duplicates(inplace=True)
    df_companies.drop_duplicates(subset=["symbol"], keep="last", inplace=True)
    df_companies.drop("date", axis=1, inplace=True)
    df_companies.set_index("symbol", inplace=True)
    db.df_write(df_companies, "companies", commit=True)
