from .spark import spark_ddl_str
# from .sql import *
# from .tsv import *
# from .comment_on import *

def render(table, format_type="tsv"):
    """
    Render table schema in specified format.
    :param table: Table object
    :param format_type: Format type ("sql", "tsv", "comment_on")
    :return: Formatted schema string
    """
    match format_type.lower():
        case "spark":
            return spark_ddl_str(table)
        # case "tsv":
        #     return render_tsv(table)
        # case "comment_on":
        #     return render_comment_on(table)
        case _:
            raise ValueError(f"Unsupported format type: {format_type}")