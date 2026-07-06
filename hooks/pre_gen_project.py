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
