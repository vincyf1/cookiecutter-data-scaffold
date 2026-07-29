import pytest

from {{ cookiecutter.project_slug }}.batch.config import (
    CONFIG_PATH,
    MissingConfigError,
    get_setting,
    load_settings,
)


def test_shipped_config_declares_the_settings_the_pipeline_reads():
    settings = load_settings()
    assert "output_path" in settings
    assert "compression" in settings


def test_get_setting_returns_a_declared_value():
    assert get_setting("output_path").endswith(".parquet")


def test_get_setting_names_the_missing_setting_and_what_is_declared():
    # This message is the entire evidence the healing agent starts from, so
    # it has to name the setting that was asked for, the settings that do
    # exist, and the file they live in.
    with pytest.raises(MissingConfigError) as excinfo:
        get_setting("output_file")

    message = str(excinfo.value)
    assert "output_file" in message
    assert "output_path" in message
    assert CONFIG_PATH.name in message


def test_get_setting_reads_the_file_it_is_given(tmp_path):
    config = tmp_path / "pipeline.toml"
    config.write_text('[pipeline]\noutput_path = "elsewhere.parquet"\n')
    assert get_setting("output_path", config) == "elsewhere.parquet"
