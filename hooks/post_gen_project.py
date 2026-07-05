import os
import shutil

PROJECT_DIR = os.path.realpath(os.curdir)
SLUG = "{{ cookiecutter.project_slug }}"

REMOVAL_RULES = [
    ("{{ cookiecutter.include_batch }}" == "True", [
        f"src/{SLUG}/batch",
        "tests/test_batch_dag.py",
    ]),
    ("{{ cookiecutter.include_streaming }}" == "True", [
        f"src/{SLUG}/streaming",
        "tests/test_streaming_consumer.py",
    ]),
    ("{{ cookiecutter.include_lakehouse }}" == "True", [
        f"src/{SLUG}/lakehouse",
        "tests/test_lakehouse_writer.py",
        "transformation/models/staging/sources.yml",
    ]),
    ("{{ cookiecutter.include_dbt }}" == "True", [
        "transformation",
    ]),
]


def remove(relative_path):
    full_path = os.path.join(PROJECT_DIR, relative_path)
    if os.path.isdir(full_path):
        shutil.rmtree(full_path)
    elif os.path.isfile(full_path):
        os.remove(full_path)


def main():
    for keep, paths in REMOVAL_RULES:
        if not keep:
            for path in paths:
                remove(path)


if __name__ == "__main__":
    main()
