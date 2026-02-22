import pandas as pd
from concurrent.futures import ThreadPoolExecutor

def read_excel_parallel(file_paths):
    dataframes = []

    def read_file(path):
        try:
            return pd.read_excel(path, engine='openpyxl')
        except:
            return None

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(read_file, file_paths))

    for df in results:
        if df is not None:
            dataframes.append(df)

    return dataframes
