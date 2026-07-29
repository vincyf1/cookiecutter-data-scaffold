import re
import sys

PROJECT_SLUG = {{ cookiecutter.project_slug | tojson }}
VALID_SLUG_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

if not VALID_SLUG_RE.match(PROJECT_SLUG):
    print(
        f"ERROR: '{PROJECT_SLUG}' is not a valid Python package name.\n"
        "project_name must produce a project_slug containing only letters, "
        "digits, and underscores, and must not start with a digit."
    )
    sys.exit(1)

INCLUDE_AGENTIC = {{ cookiecutter.include_agentic }}
INCLUDE_BATCH = {{ cookiecutter.include_batch }}

if INCLUDE_AGENTIC and not INCLUDE_BATCH:
    print(
        "ERROR: include_agentic requires include_batch.\n"
        "The Agentic Pattern heals a failing batch DAG; with no DAG scaffolded "
        "there is nothing for it to observe. Enable include_batch, or set "
        "include_agentic=false."
    )
    sys.exit(1)
