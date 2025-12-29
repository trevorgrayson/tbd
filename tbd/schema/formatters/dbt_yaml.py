from collections import OrderedDict


def to_source_yaml(table, database_name=None):
    """
    Convert to DBT Source YAML format.

    # TODO missing convert_mysql2spark

    :return:
    """
    sources_mock = {
        "sources": {
            "columns": [
                dict(OrderedDict({
                    "name": col.name,
                    "type": col.dtype,
                    "description": col.dtype
                })) for col in table.columns
            ]
        }
    }

    cols = [f"""
    - name: {col.name}
      type: {col.dtype}
      description:
    """ for col in table.columns]
    return f"""
version: 2

sources:
- name: {database_name or ''}
  database: {database_name or ''}
  tables:

  - name: {table.name}
    description: "Auto-Generated Documentation from TBD"
    columns:
    {"".join(cols)}
"""