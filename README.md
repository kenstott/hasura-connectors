# hasura-connectors
hasura-connectors

## Summary

Just a few ideas on exploiting the hasura connector SDK (which is based on exposing XML files). This is a fork of the repo for the official hasura connector SDK. There are 3 connectors in this forked repo - "reference" is the original connector for XML files, "file-connector" provides JSON, XLSX and CSV file support, and "spark-connector", which adds the ability to connect to a spark cluster.

## Changes

* Added more support for async operations within the original connector core code
* Added JSON, XLSX and CSV file support to a new file-connector
* Created a spark-connector - based on connection to a livy server. Testing locally is a little complex - need to setup a spark instance and a livy server first.
* Connectors are read-only - there is no write-back in any connector
* Supports relationships between all connector types
* spark-connector uses dataloader pattern to improve performance
