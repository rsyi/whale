from jinja2 import Environment, BaseLoader
from pathlib import Path
from termcolor import colored
from whale.utils.paths import MACROS_DIR

DEFAULT_TEMPLATE_NAME = "default.sql"
FAILING_COLOR = "red"
PASSING_COLOR = "green"


def template_query(query, connection_name=""):
    """
    Takes the provided query, looks for a connection_name.sql file containing
    Jinja templating, combines these strings, and runs Jinja2 to render the
    templated query.
    """
    # First determine the connection type, and look for a "connection_name.sql" file in templates.
    template_file_path = MACROS_DIR / ((connection_name or "") + ".sql")
    is_template_file_path_found = template_file_path.is_file()

    if is_template_file_path_found:
        # Load this default file and prepend to the provided query.
        with open(template_file_path, "r") as f:
            template = f.read()
        query = "\n".join([template, query])

    # Template the query with Jinja.
    template = Environment(loader=BaseLoader).from_string(query)
    templated_query = template.render()
    return templated_query


def validate_templates():
    if MACROS_DIR.is_dir():
        for template_file in MACROS_DIR.glob("**/*"):
            _validate_and_print_result(template_file)
    else:
        warning_text = textwrap.dedent(
            """
        No templates folder found.

        To create templates, create .sql files in ~/.whale/templates/, named after your connection names. For example, if you have a connection named `bq-1`, create a file `bq-1.sql`.

        To see your available connection names, run `wh connections`.
        """
        )
        print(warning_text)


def _validate_and_print_result(template_file: Path):
    relative_file_path = template_file.relative_to(MACROS_DIR)
    with open(template_file, "r") as f:
        template = f.read()
        try:
            Environment(loader=BaseLoader).from_string(template).render()
            status_color = PASSING_COLOR
            status_symbol = colored("✓", status_color)
            error = None
        except Exception as e:
            status_color = FAILING_COLOR
            status_symbol = colored("x", status_color)
            error = e

    print(f"[{status_symbol}] {relative_file_path}")

    if error:
        print(f"Error: {error}")

    return error
