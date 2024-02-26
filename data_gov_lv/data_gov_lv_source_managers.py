from typing import Any, List
from xml.dom import minidom
from xml.dom.minidom import Document

import pandas as pd
import requests
from dagster import InputContext, IOManager, OutputContext
from duckdb import DuckDBPyRelation

from data_gov_lv.duckdb.url_loader import duckdb_load_from_csv_url, duckdb_load_from_json_url
from data_gov_lv.url_cache.cache_paths import dagster_cache_root


class JsonInputManager(IOManager):
    def load_input(self, context: InputContext) -> object:
        url = context.upstream_output.metadata["url"]
        get = requests.get(url)
        get.raise_for_status()
        return get.json()

    def handle_output(self, context: "OutputContext", obj: Any) -> None: ...


class MinidomInputManager(IOManager):
    def load_input(self, context: InputContext) -> Document:
        url = context.upstream_output.metadata["url"]
        get = requests.get(url)
        get.raise_for_status()
        data = get.content
        return minidom.parseString(data)

    def handle_output(self, context: "OutputContext", obj: Any) -> None: ...


class PandasCsvInputManager(IOManager):
    def load_input(self, context: InputContext) -> pd.DataFrame:
        url = context.upstream_output.metadata["url"]
        sep = context.upstream_output.metadata["csv_separator"]
        enc = context.upstream_output.metadata.get("csv_encoding", 'utf-8')
        return pd.read_csv(
            url,
            sep=sep,
            encoding=enc,
            comment='#',
        )

    def handle_output(self, context: "OutputContext", obj: Any) -> None:  ...


class DuckDbCsvInputManager(IOManager):
    def load_input(self, context: InputContext) -> List[DuckDBPyRelation] | DuckDBPyRelation:
        url = context.upstream_output.metadata["url"]
        return duckdb_load_from_csv_url(dagster_cache_root(context.instance), url)

    def handle_output(self, context: "OutputContext", obj: Any) -> None:
        ...


class DuckDbJsonInputManager(IOManager):
    def load_input(self, context: InputContext) -> DuckDBPyRelation:
        url = context.upstream_output.metadata["url"]
        return duckdb_load_from_json_url(dagster_cache_root(context.instance), url)

    def handle_output(self, context: "OutputContext", obj: Any) -> None:
        ...
