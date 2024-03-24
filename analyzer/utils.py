from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import pandas as pd
from tqdm import tqdm



def multi_read_df_from_paths(paths: list[str], read_function = pd.read_pickle) -> list[pd.DataFrame]:
    dfs = []
    num_cpus = multiprocessing.cpu_count()
    with ThreadPoolExecutor(max_workers=num_cpus) as executor:
        futures = []
        for path in paths:
            futures.append(executor.submit(read_function, path))
        for future in tqdm(futures):
            dfs.append(future.result())
    return dfs
