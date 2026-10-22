"""The JSON Schemas pysparring ships, each read by the name `pysparring schemas show` takes."""

import importlib.resources


def schema_names() -> tuple[str, ...]:
    files = importlib.resources.files(__name__).iterdir()
    return tuple(sorted(entry.name.removesuffix('.json') for entry in files if entry.name.endswith('.json')))


def schema_text(name: str) -> str:
    return importlib.resources.files(__name__).joinpath(f'{name}.json').read_text()
