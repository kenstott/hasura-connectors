# Data Connector File Implementation

This directory contains a CSV/XLSX/JSON implementation of the Data Connector agent specification which fetches its data from a
spark context.

## Requirements

* NodeJS 16

## Build & Run

```
> npm install
> npm start
```

## Docker Build & Run

```
> docker build . -t dc-reference-agent:latest
> docker run -it --rm -p 8100:8100 dc-reference-agent:latest
```

## Dataset

The dataset exposed by the agent is sourced from src/data/databases. The directory under test/ is
considered a list of databases. Everything under the database - includes the files to load - and the config.json
file for the database.

## Configuration

The reference agent supports some configuration properties that can be set via the `value` property of `configuration`
on a source in Hasura metadata. The configuration is passed to the agent on each request via
the `X-Hasura-DataConnector-Config` header.

The configuration that the reference agent can take supports two properties:

* `tables`: This is a list of table names that should be exposed by the agent. If omitted all dataset tables are
  exposed. If specified, it filters all available table names by the specified list.
* `schema`: If specified, this places all the tables within a schema of the specified name. For example, if `schema` is
  set to `my_schema`, all table names will be namespaced under `my_schema`, for example `["my_schema","Employee"]`. If
  not specified, then tables are not namespaced, for example `["Employee"]`.

Here's an example configuration that only exposes the Employee and Department tables, and namespaces them
under `my_schema`:

```json
{
  "tables": ["Employee", "Department"],
  "schema": "my_schema"
}
```

Here's an example configuration that exposes all tables, un-namespaced:

```json
{}
```

# Additional Hasura CSV Connector Features

## Files

**Note:** Files are automatically profiled. It looks for a column with unique values and assumes 
it is the primary key. Substitutes the boolean and null synonyms for string columns. After profiling, it determines the best
data type. String is the final backstop for a data type. But does a good job of finding dates, booleans and numbers.

| Type | Description                                                    |
|------|----------------------------------------------------------------|
| CSV  | handles CSV files                                              |
| JSON | works, but non-primitives are converted to a string equivalent |
| XML  | Needs to be added.                                             |
| XLSX | Works, currently loads every tab in the workbook.              |

## Environment Variables

| Name                  | Description                                                                                                                                                   |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SPARK_CONNECTOR_FILES | A path to a local config file, for additional spark connector features, and additional local JSON or CSV files that you want to include in your spark session |
| LIVY_URI              | A URI to the [Livy server](https://livy.apache.org/)                                                                                                          |

## config.json

```json5
{
  // synonyms to convert to NULL on loading datasets
  "nulls": [
    "-",
    "",
    " - "
  ],
  // synonyms to convert to boolean on loading datasets
  "booleans": {
    "positive": [
      "yes",
      "true"
    ],
    "negative": [
      "no",
      "false"
    ]
  },
  
  // self-explanatory - but this lets you create
  // primary key/foreign key relationships that will be exposed
  // to the Hasura metadata loader to suggest relationships
  "schema": {
    "tables": [
      {
        "name": [
          "employee"
        ],
        "primary_key": ["EMPLOYEE_ID"],
        "description": "Custom description",
        "foreign_keys": {
          "Manager": {
            "column_mapping": {
              "MANAGER_ID": "EMPLOYEE_ID"
            },
            "foreign_table": [
              "employee"
            ]
          },
          "Department": {
            "column_mapping": {
              "DEPARTMENT_ID": "DEPARTMENT_ID"
            },
            "foreign_table": [
              "department"
            ]
          }
        }
      },
      {
        "name": [
          "department"
        ],
        "description": "Custom description",
        "primary_key": ["DEPARTMENT_ID"],
        "foreign_keys": {
          "Manager": {
            "column_mapping": {
              "MANAGER_ID": "EMPLOYEE_ID"
            },
            "foreign_table": [
              "employee"
            ]
          }
        }
      }
    ]
  }
}
```

## To Be Completed

* XML - TBD
* JSON works - except - only for primitives - it turns those into strings.
* Need to do more work to improve the overall logic of matching physical names (file & file system) to graphql names. It's very messy right now. Lots of duplication of code.
