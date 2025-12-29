# tbd

Data utilities for defining datasets and aggregating metadata.

Key Features:
- hub-and-spoke schema management
  - define schemas
  - document schemas
  - make new schemas available for query
- exposures - define uses of data (dashboards, reports, ML models, etc)

Try `tbd -h` on the commandline to get started.

## Schemas

Utilities can be used to gather, define, and transfer schemas between systems.

- Get established schemas from databases: MySQL, Spark/Databricks, csv
- pidgin schema definitions

A "hub & spoke" model is used to effectively gather and transfer to other systems.

### `tbd import` schema

Schemas can be imported.

- [x] TSV, CSV
- [x] YAML
- [x] DBT Sources (virtually all data products)
- [ ] Spark/Databricks
- [ ] ANSI SQL DDLs
- [ ] MySQL
- JSON

### `tbd show`

`tbd show` will list the schemas presently designed in "the hub" (see Concepts.)
`tbd show {schema}` will display the detail of schema definition.

Replace `tbd show` with `tbd edit` to modify the schema.

### `tbd edit`

Update an existing schema. See `tbd show`.

### `tbd export` schema

Schemas can be exported.

- [x] Spark DDL Strings
- [ ] ANSI SQL DDLs

## Exposures

Define uses of data.  Examples of exposures include: 
dashboards, reports, ML models, and data products.

Defining owners and impact before making changes will 
help communicate changes to stakeholders.


## Concepts

### the "hub"

the `datahub` folder acts as a working directory, or potentially the definition of, your datasets.

The hub is presumed to be the target if the `origin` or `destiniation` is left empty.

You may define:

* `source` data (data which enters the system),
* `sink`s, or data `exposure`s (uses of data),
* pipelines/transforms, or
* lineage and relationships between data.
