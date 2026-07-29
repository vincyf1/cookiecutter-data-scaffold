"""Pipeline settings, read from batch/pipeline.toml.

Without the Lakehouse Pattern there is no DDL for a record to drift away
from, so this file plays the role create_tables.sql plays there: the single
declared source a lookup is checked against. Deriving the runtime error
from the file itself means the error message and the ground truth cannot
disagree.
"""

import tomllib
from pathlib import Path

CONFIG_PATH = Path(__file__).parent / "pipeline.toml"
SECTION = "pipeline"


class MissingConfigError(Exception):
    """A caller asked for a setting the pipeline config does not declare."""


def load_settings(path: Path = CONFIG_PATH) -> dict:
    return tomllib.loads(Path(path).read_text()).get(SECTION, {})


def get_setting(name: str, path: Path = CONFIG_PATH) -> str:
    """Resolve one setting, or say precisely what was missing and what exists.

    The message is the agent's entire starting evidence, so it names the
    setting that was asked for, the settings that are declared, and where
    they are declared -- the same three things SchemaDriftError names.
    """
    settings = load_settings(path)
    if name not in settings:
        raise MissingConfigError(
            f"pipeline config declares no setting '{name}'; "
            f"{Path(path).name} declares {sorted(settings)}. "
            "Either the caller renamed a setting or the config file is out of date."
        )
    return settings[name]
