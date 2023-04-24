# Data Connector Spark Implementation

This directory contains a spark implementation of the Data Connector agent specification which fetches its data from a
spark context.

In order to develop/test locally - you must have access to a spark instance with a Livy server.
You can set one up locally. This was how I set up a local environment:

| Package    | Description                                                                                                                                                                                               |
|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Hadoop** | downloaded: [hadoop-2.7.3.tar.gz](https://archive.apache.org/dist/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz) and unzipped. Click [here](https://www.edureka.co/blog/install-hadoop-single-node-hadoop-cluster#:~:text=Install%20Hadoop%201%20Step%201%3A%20Click%20here%20to,mentioned%20below%20inside%20configuration%20tag%3A%20...%20More%20items) for configuration details. (run sbin/start-all.sh) |
| **Spark**  | downloaded: [spark-2.4.0-bin-hadoop2.7.tgz](https://archive.apache.org/dist/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz) and unzipped (run libexec/sbin/start-all.sh)                                 |
| **Livy**   | downloaded: [apache-livy-0.7.1-incubating-bin](https://www.apache.org/dyn/closer.lua/incubator/livy/0.7.1-incubating/apache-livy-0.7.1-incubating-bin.zip) and unzipped (run ./bin/livy-server start)     |

You may need to set up several environment variables when running Livy locally.
```
SCALA_HOME=<?>/scala-2.11.12
SPARK_HOME=<?>/spark-2.4.0-bin-hadoop2.7
LIVY_HOME=<?>/apache-livy-0.7.1-incubating-bin
HADOOP_COMMON_HOME=<?>/hadoop/2.7.3
HADOOP_HOME=<?>/hadoop/2.7.3
HADOOP_CONF_HOME=${HADOOP_HOME}/libexec/etc/hadoop
```
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

The dataset exposed by the agent is sourced from src/data/test

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

# Additional Hasura Spark Connector Features

## Other

The original data connector reference project is synchronous and assumed responsibility for all computation.

In order to use a remote async service (like Livy/Spark) it meant a significant refactor of the reference version.

The second challenge is pushing down operations to spark to improve performance for large datasets. Currently, the spark
connector pushes down selection for a single table and manages sorting, filtering, column selection, and pagination.

Nested relationships are handled by the data connector and uses a dataloader pattern to avoid the N+1 problem. Would be
an improvement to push relationships down to spark server.
Aggregates are still computed by the data connector instead of spark.

## Files

**Note:** You can use local files for testing. But local files require that the spark server is hosted on your device.
Otherwise, add files into the "remoteFiles" part of the spark config file.

| Type | Description                                                                                                                                                                                                                                                                   |
|------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CSV  | handles CSV files pretty well - but relies on schema inference from spark, which is not great. Does not identify primary keys, does not identify dates. If there are any conventions to indicate no number - like spaces or dashes - the schema will classify it as a string. |
| JSON | works, but excludes all variables at the document root that are not primitives. Although spark supports embedded JSON columns - it does not seem that Hasura does. Might be able to flatten JSON file before loading into a dataframe.                                        |
| XML  | It ought to work, but have not been able to locally get XML support working in Spark (library version issues). May also require additional hints in the spark config file - like rowTag or rootTag.                                                                           |
| XLSX | Conceptually, ought to work. Need an XLSX driver for spark.read.load in order to test. May require additional additional spark config options to identify specific tabs and/or ranges to extract from.                                                                        |

## Environment Variables

| Name                  | Description                                                                                                                                                   |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SPARK_CONNECTOR_FILES | A path to a local config file, for additional spark connector features, and additional local JSON or CSV files that you want to include in your spark session |
| LIVY_URI              | A URI to the [Livy server](https://livy.apache.org/)                                                                                                          |

## config.json

```json5
{
  // not working yet - the idea is to create
  // synonyms to convert to NULL on loading datasets
  "nulls": [
    "-",
    "",
    " - "
  ],
  // not working yet - the idea is to create
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
  // enter any URIs support by spark.read.load()
  // for example - hdfs:///my-csv.csv or ftp://name:password@my-site.com/my-csv.csv
  "remoteFiles": ["https://cdn.wsform.com/wp-content/uploads/2020/06/industry.csv"],
  
  // if a foreign key is to be used to link to
  // a target table - and there is a data type mismatch
  // that came from spark's schema inference
  // this will convert the keys to either a string, integer or float
  // to attempt to match the target table's data type for the primary ely
  "coerceForeignKeys": {
    "employee": {
      "MANAGER_ID": "integer"
    },
    "department": {
      "MANAGER_ID": "integer"
    }
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

* Cannot figure out how aggregates are defined by the agent. They just seem to show up. They also will only add string
  types to the aggregate operations. Need to figure out how to expose numbers to the aggregates. Need to figure out how
  to push down aggregate computation to spark.
* XML might work - need to get xml loader locally to test it. May require additional hints in spark config file - around
  root tag and row tag(s)
* JSON works - except - only for primitives. Spark dataframe allows for JSON columns - but it does not seem that Hasura
  does. Need to figure this out. Potentially, could create a scala script to flatten the JSON file before creating final
  dataset.
* Livy server - have not added in authorization. You may need to change the code to include authorization if you are
  using a secure Livy server.
* Need to do more work to improve the overall logic of matching physical names (spark) to graphql names. It's very messy right now. Lots of duplication of code.
