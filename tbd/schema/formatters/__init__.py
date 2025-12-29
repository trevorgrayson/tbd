from .spark import spark_ddl_str
from .dbt_yaml import to_source_yaml
# from .sql import *
# from .tsv import *
# from .comment_on import *

def render(table, format_type="tsv", database_name=None):
    """
    Render table schema in specified format.
    :param table: Table object
    :param format_type: Format type ("sql", "tsv", "comment_on")
    :param database_name: Optional database name for certain formats
    :return: Formatted schema string
    """
    match format_type.lower():
        case "spark":
            return spark_ddl_str(table)
        case "dbt":
            return to_source_yaml(table, database_name=database_name)
        # case "tsv":
        #     return render_tsv(table)
        # case "comment_on":
        #     return render_comment_on(table)
        case _:
            raise ValueError(f"Unsupported format type: {format_type}")