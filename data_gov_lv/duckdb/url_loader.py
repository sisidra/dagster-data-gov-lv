import tempfile
from itertools import chain
from pathlib import Path
from typing import List

import duckdb
import pandas as pd

from data_gov_lv.url_cache.cache_paths import cache_path
from data_gov_lv.url_cache.fetcher import cached_get


def duckdb_load_from_csv_url(cache_dir: Path, url: str) -> List[duckdb.DuckDBPyRelation] | duckdb.DuckDBPyRelation:
    file_path = cached_get(cache_dir, url)

    if url.endswith("fromexcelnouns.csv"):
        # fk it!
        return duckdb.from_query(
            f"FROM read_csv('{file_path}',"
            " columns={'noun': 'VARCHAR',"
            "          'declension': 'VARCHAR',"
            "          'diminutive': 'VARCHAR',"
            "          'declension2': 'VARCHAR',"
            "          'stem': 'VARCHAR'},"
            " ignore_errors=true,"
            " rejects_table='rejects_table')"
        )

    # Daži glabā vairākas daļas atdalītas ar tukšu rindu... :facepalm:
    # Šķiet, ka šiem failiem ir komentāru headeris
    text = file_path.read_text()
    if text.startswith("#"):
        parts = text.split("\n\n")
    else:
        parts = [text]
    # Daži glabā velns-viņ-zin-ko-un-kā... :bigfacepalm:
    # https://data.gov.lv/dati/dataset/7f7841ba-348a-46f1-852d-90d76e54cc09/resource/16d17f1b-e449-486b-b6c6-c53c29f043c7/download/administratvo-prkpumu-statistika-2022.-2.ceturksnis.csv
    parts = list(chain.from_iterable(part.split("\n;\n") for part in parts))

    if len(parts) == 1:
        try:
            return duckdb.from_csv_auto(str(file_path), header=True)
        except duckdb.InternalException:
            # https://data.gov.lv/dati/dataset/42495f6a-e104-453e-af17-9aa7e4a7faf1/resource/a244f9aa-b054-4c68-bfc5-0e7224b12a59/download/pil_izslgana_2019.csv
            return duckdb.from_csv_auto(str(file_path), header=True, delimiter=";")
        except duckdb.InvalidInputException:
            try:
                # https://data.gov.lv/dati/dataset/5b2f8783-05b7-4ee9-8426-6d8b4e48f810/resource/8967d5a4-c13d-4c91-bbf1-90ab0ffce5a7/download/project.csv
                df = pd.read_csv(file_path)
                return duckdb.from_df(df)
            except Exception as e:
                raise Exception(f"URL: {url} Cache: {file_path}") from e

    result = []
    url_cache_dir = cache_path(cache_dir, url)
    for i in range(len(parts)):
        part = parts[i]
        if part.startswith("#"):
            # Izmetam komentāru daļas, kaut arī tās nav csv standarts
            continue
        if part == "":
            # ...
            continue

        with (url_cache_dir / f"part-{i}.csv").open("w") as fp:
            fp.write(part)

        try:
            df = duckdb.from_csv_auto(fp.name, header=True)
        except duckdb.InvalidInputException as e:
            raise Exception(f"URL: {url} Part cache: {fp.name}") from e

        result.append(df)

    return result


def duckdb_load_from_json_url(cache_dir: Path, url: str) -> duckdb.DuckDBPyRelation:
    file_path = cached_get(cache_dir, url)
    file_path = str(file_path.resolve())
    return duckdb.read_json(file_path)
