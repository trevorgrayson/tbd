"""
Spark Formatters

"""


def spark_ddl_str(table):
    """
    Generate Spark DDL String for the given table.
    :param table: Table object
    :return: Spark DDL string
    """
    cols = [f"{c.name} {c.dtype}" for c in table.columns]
    ddl = ", ".join(cols)

    return ddl
