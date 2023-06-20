# hasura-connectors
hasura-connectors

Just a few ideas on exploiting the hasura connector SDK (which is based on exposing XML files).

* Added more support for async operations within the connector core code
* Added JSON, XLSX and CSV file support to a new file connector
* Created a Spark connector - based on connection to a livy server. Testing locally is a little complex - need to setup a spark instance and a livy server first.
