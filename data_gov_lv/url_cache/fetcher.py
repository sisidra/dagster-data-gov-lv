import codecs
from pathlib import Path

import requests

from data_gov_lv.url_cache.cache_paths import cache_path


def cached_get(cache_root: Path, url: str) -> Path:
    url_cache_dir = cache_path(cache_root, url)
    url_cache_dir.mkdir(parents=True, exist_ok=True)

    url_cache_file_name = Path("original" + Path(url).suffix)
    url_cache_path = url_cache_dir / url_cache_file_name

    if url_cache_path.exists():
        return url_cache_path

    response = requests.get(url)
    response.raise_for_status()

    response.encoding = response.apparent_encoding

    data = response.text
    if data.startswith(codecs.BOM_UTF8.decode("utf-8")):
        data = data[len(codecs.BOM_UTF8.decode("utf-8")):]

    url_cache_path.write_text(data)

    return url_cache_path
