from dagster import Definitions, in_process_executor

from data_gov_lv import data_gov_lv_assets
from data_gov_lv.data_gov_lv_source_managers import (
    MinidomInputManager,
    DuckDbCsvInputManager,
    DuckDbJsonInputManager,
)

defs = Definitions(
    assets=[data_gov_lv_assets.CKANCacheableAssetsDefinition("data.gov.lv")],
    resources={
        "gov_xml_io_manager": MinidomInputManager(),
        "gov_json_io_manager": DuckDbJsonInputManager(),
        "gov_csv_io_manager": DuckDbCsvInputManager(),
    },
    executor=in_process_executor,
)
