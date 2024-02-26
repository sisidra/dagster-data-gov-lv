import hashlib
import json
import os
import re
from itertools import groupby
from pathlib import Path
from typing import Any, List, Sequence, Optional, Tuple, Iterator, Dict, cast
from xml.dom.minidom import Document

import requests
from ckanapi import RemoteCKAN
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    DagsterInvariantViolationError,
    asset, MetadataValue, TableSchemaMetadataValue, TableSchema, TableColumn, TableColumnConstraints, DagsterInstance,
    get_dagster_logger, UrlMetadataValue, )
from dagster._core.definitions.asset_spec import (
    SYSTEM_METADATA_KEY_ASSET_EXECUTION_TYPE,
    AssetExecutionType,
)
from dagster._core.definitions.cacheable_assets import CacheableAssetsDefinition, AssetsDefinitionCacheableData
from dagster._core.definitions.utils import DEFAULT_OUTPUT
from dagster._core.types.dagster_type import resolve_dagster_type, DagsterType
from dagster._serdes import unpack_value, pack_value
from duckdb import DuckDBPyRelation
from duckdb.typing import DuckDBPyType
from filelock import FileLock
from unidecode import unidecode

from data_gov_lv.duckdb.url_loader import duckdb_load_from_csv_url, duckdb_load_from_json_url
from data_gov_lv.url_cache.cache_paths import dagster_cache_root, cache_path
import progressbar as pb

dagster_home = DagsterInstance.get().root_directory
cache_dir = dagster_cache_root(DagsterInstance.get())
compute_cacheable_data_lock_file = cache_dir / "data-gov-lv.cache.lock"
compute_cacheable_data_cache_file = cache_dir / "data-gov-lv.cache.json"


def _dagster_identifier(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", unidecode(name))


class CKANCacheableAssetsDefinition(CacheableAssetsDefinition):
    gov: RemoteCKAN = RemoteCKAN("https://data.gov.lv/dati/lv")

    def _try_conforms_to_schema(
            self,
            conforms_to: Optional[str],
    ) -> Tuple[Optional[str], Optional[TableSchemaMetadataValue]]:
        if conforms_to is None or conforms_to == "":
            return None, None

        if not conforms_to.startswith("https://") and not conforms_to.startswith("http://"):
            return conforms_to, None

        if not conforms_to.endswith(".json"):
            if conforms_to == "https://www.epakalpojumi.lv/odata/service/$metadata":
                return None, None
            print("TODO: conforms to: " + conforms_to)
            return None, None

        csvw_response = requests.get(conforms_to)
        if csvw_response.status_code == 404:
            return None, None

        csvw_response.encoding = csvw_response.apparent_encoding
        csvw = csvw_response.json()
        description = csvw.get("dc:title")
        schema = MetadataValue.table_schema(
            TableSchema(
                columns=[
                    TableColumn(
                        name=column["name"],
                        type=column["datatype"] if isinstance(column["datatype"], str) else column["datatype"]["base"],
                        description=column.get("dc:description") or column["titles"],
                        constraints=TableColumnConstraints(
                            nullable=not column.get("required", False),
                            other=[json.dumps(fk) for fk in column.get("foreignKeys", [])]
                        )
                    )
                    for column in csvw["tableSchema"]["columns"]
                ]
            )
        )
        return description, schema

    def _duckdb_columns(self, prefix, col_types: List[Tuple[str, DuckDBPyType]]) -> Iterator[TableColumn]:
        for name, col_type in col_types:
            yield TableColumn(
                name=f"{prefix}{name}",
                type=col_type.id,
            )
            if col_type.id == "struct":
                yield from self._duckdb_columns(f"{prefix}{name}.", col_type.children)
            if col_type.id == "list":
                _, list_type = col_type.children[0]
                yield from self._duckdb_columns(f"{prefix}{name}", [("[]", list_type)])

    def _try_csv_schema(self, url) -> Tuple[Optional[str], Optional[TableSchemaMetadataValue]]:
        datasets = duckdb_load_from_csv_url(cache_dir, url)

        if isinstance(datasets, DuckDBPyRelation):
            col_types: List[Tuple[str, DuckDBPyType]] = zip(datasets.columns, datasets.types)
            columns = list(self._duckdb_columns("", col_types))
            return "CSV File", MetadataValue.table_schema(TableSchema(columns=columns))

        columns = []
        for i in range(len(datasets)):
            columns.append(
                TableColumn(
                    name=f"#{i}",
                    type=f"duckdb.DuckDBPyRelation",
                    description=f"Daži csv faili satur vairākas daļas, šī ir {i} daļa.",
                    constraints=TableColumnConstraints(nullable=False),
                )
            )
            dataset = datasets[i]
            col_types: List[Tuple[str, DuckDBPyType]] = zip(dataset.columns, dataset.types)
            columns.extend(self._duckdb_columns("", col_types))

        return (
            "CSV File with multiple parts, all returned in a list",
            MetadataValue.table_schema(TableSchema(columns=columns))
        )

    def _try_json_schema(self, url) -> Tuple[Optional[str], Optional[TableSchemaMetadataValue]]:
        dataset = duckdb_load_from_json_url(cache_dir, url)

        columns = []
        col_types: List[Tuple[str, DuckDBPyType]] = zip(dataset.columns, dataset.types)
        columns.extend(self._duckdb_columns("", col_types))

        return "JSON File", MetadataValue.table_schema(TableSchema(columns=columns))

    def _try_url_schema(self, url: Optional[str]) -> Tuple[Optional[str], Optional[TableSchemaMetadataValue]]:
        if url is None or url == "":
            return None, None

        cache_file = cache_path(cache_dir, url) / "schema.json"
        if cache_file.exists():
            with cache_file.open(encoding="UTF-8") as fp:
                cache_data = json.load(fp)
                return cache_data["description"], unpack_value(cache_data["schema"])

        schema = None
        description = None
        if url.endswith(".csv"):
            description, schema = self._try_csv_schema(url)
        if url.endswith(".json"):
            description, schema = self._try_json_schema(url)

        if schema is not None:
            cache_data = {
                "url": url,
                "description": description,
                "schema": pack_value(schema),
            }
            with cache_file.open("w", encoding="UTF-8") as fp:
                json.dump(cache_data, fp, indent=2)

        return description, schema

    def _detect_schema(self, resource) -> Tuple[Optional[str], Optional[TableSchemaMetadataValue]]:
        try:
            description, schema = self._try_conforms_to_schema(resource.get("conformsTo"))
        except:
            raise Exception("conformsTo: " + resource.get("conformsTo"))

        if schema is None:
            description, schema = self._try_url_schema(resource.get("url"))
        return description, schema

    def _load_resource(self, package, resource) -> Sequence[AssetsDefinitionCacheableData]:
        url = resource["url"]
        if url == "https://odata.ievp.gov.lv/ReportOdata/":
            # TODO ...
            get_dagster_logger().info("TODO: Support https://odata.ievp.gov.lv/ReportOdata/")
            return

        namespace = _dagster_identifier(package["name"])
        resource_format = _dagster_identifier(resource["format"])
        name = _dagster_identifier(resource.get("name") or resource["id"])

        schema_description, schema_definition = self._detect_schema(resource)

        asset_key = AssetKey([namespace, resource_format, name])

        yield AssetsDefinitionCacheableData(
            keys_by_output_name={DEFAULT_OUTPUT: asset_key},
            metadata_by_output_name={DEFAULT_OUTPUT: {
                "url": MetadataValue.url(url),
                "resource/id": resource["id"],
                "resource/package_id": resource["package_id"],
                "resource/created": resource["created"],
                "resource/name": resource["name"],
                "resource/state": resource["state"],
                "package/name": package["name"],
                "package/notes": package["notes"],
                "package/maintainer_email": package["maintainer_email"],
                "package/org/name": package["organization"]["name"],
                "package/org/title": package["organization"]["title"],
                "package/org/description": package["organization"]["description"],
            }},
            extra_metadata={
                "description": resource["description"],
                "schema_definition": schema_definition,
                "schema_description": schema_description,
            }
        )

    def _load_package(self, package_id) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
        package = self.gov.action.package_show(id=package_id)
        raw_resources = package["resources"]
        metadata_resources = {r.get("conformsTo", "") for r in raw_resources}
        non_metadata_resources = [
            r
            for r in raw_resources
            if r.get("url") not in metadata_resources and not r.get("url", "").endswith("_metadata.json")
        ]
        grouped_resources = groupby(
            sorted(non_metadata_resources, key=lambda r: r.get("name") or r["id"]),
            key=lambda r: r.get("name") or r["id"]
        )
        resources = [
            (package, max(values, key=lambda r: r["last_modified"] or r["created"]))
            for _, values in grouped_resources
        ]
        return resources

    def compute_cacheable_data(self) -> Sequence[AssetsDefinitionCacheableData]:
        with FileLock(compute_cacheable_data_lock_file, timeout=60 * 10):
            if compute_cacheable_data_cache_file.exists():
                if compute_cacheable_data_cache_file.lstat().st_mtime > Path(__file__).lstat().st_mtime:
                    with open(compute_cacheable_data_cache_file, "r") as f:
                        get_dagster_logger().info("Load file cache")
                        return unpack_value(json.load(f))
                else:
                    get_dagster_logger().info("Reload cache, loader changed")

            packages = self.gov.action.package_list()

            widgets = [f'Loading packages: ', pb.Counter(), f" of {len(packages)} ", pb.Percentage(), ' ',
                       pb.Bar(right=pb.RotatingMarker()), ' ', pb.ETA()]
            timer = pb.ProgressBar(widgets=widgets, maxval=len(packages)).start()

            resources = []
            for package_idx in range(len(packages)):
                timer.update(package_idx)
                package_id = packages[package_idx]
                resources.extend(self._load_package(package_id))
            timer.finish()

            widgets = [f'Loading resources: ', pb.Counter(), f" of {len(resources)} ", pb.Percentage(), ' ',
                       pb.Bar(right=pb.RotatingMarker()), ' ', pb.ETA()]
            timer = pb.ProgressBar(widgets=widgets, maxval=len(resources)).start()

            data = []
            for i in range(len(resources)):
                timer.update(i)
                package, resource = resources[i]
                data.extend(self._load_resource(package, resource))
            timer.finish()

            get_dagster_logger().info("Save file cache")
            json_content = json.dumps(pack_value(data), indent=2)
            compute_cacheable_data_cache_file.write_text(json_content)

            return data

    def build_definitions(self, data: Sequence[AssetsDefinitionCacheableData]) -> Sequence[AssetsDefinition]:
        data = list(data)
        widgets = [f'Build definitions: ', pb.Counter(), f" of {len(data)} ", pb.Percentage(), ' ',
                   pb.Bar(right=pb.RotatingMarker()), ' ', pb.ETA()]
        timer = pb.ProgressBar(widgets=widgets, maxval=len(data)).start()

        for i in range(len(data)):
            def_data = data[i]
            timer.update(i)

            metadata = def_data.metadata_by_output_name[DEFAULT_OUTPUT]
            url = cast(metadata["url"], UrlMetadataValue).url

            io_manager_key = None
            output_type = Any
            dagster_type = None

            if url.endswith(".xml"):
                io_manager_key = "gov_xml_io_manager"
                output_type = Document
                dagster_type = resolve_dagster_type(output_type)
            elif url.endswith(".json"):
                io_manager_key = "gov_json_io_manager"
                output_type = DuckDBPyRelation
                dagster_type = DagsterType(
                    lambda _, __: True,
                    name="/".join(def_data.keys_by_output_name[DEFAULT_OUTPUT].path),
                    typing_type=output_type,
                    description=def_data.extra_metadata.get("schema_description"),
                    metadata={"schema": def_data.extra_metadata.get("schema_definition")},
                )
            elif url.endswith(".csv"):
                io_manager_key = "gov_csv_io_manager"
                output_type = List[DuckDBPyRelation] | DuckDBPyRelation
                dagster_type = DagsterType(
                    lambda _, __: True,
                    name="/".join(def_data.keys_by_output_name[DEFAULT_OUTPUT].path),
                    typing_type=output_type,
                    description=def_data.extra_metadata.get("schema_description"),
                    metadata={
                        "schema": def_data.extra_metadata.get("schema_definition"),
                        "schema1": def_data.extra_metadata.get("schema_definition"),
                        "schema2": def_data.extra_metadata.get("schema_definition"),
                    },
                )

            @asset(
                key=def_data.keys_by_output_name[DEFAULT_OUTPUT],
                metadata={
                    **metadata,
                    SYSTEM_METADATA_KEY_ASSET_EXECUTION_TYPE: AssetExecutionType.UNEXECUTABLE.value,
                },
                io_manager_key=io_manager_key,
                description=def_data.extra_metadata["description"],
                group_name="data_gov_lv",
                dagster_type=dagster_type,
            )
            def gov_asset(context: AssetExecutionContext) -> output_type:
                raise DagsterInvariantViolationError(
                    "You have attempted to execute an unexecutable asset"
                    f" {context.asset_key.to_user_string}."
                )

            yield gov_asset
        timer.finish()
