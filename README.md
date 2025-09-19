# ArangoDB-JDBC - Driver
This is a JDBC driver for the [ArangoDB](https://arangodb.com).

The driver uses the official [ArangoDB Java Driver](https://github.com/arangodb/arangodb-java-driver) and for SQL parsing the library [JSQLParser](https://github.com/JSQLParser/JSqlParser). 

Besides SQL commands, AQL commands can also be executed.

### Outer Join

A (left) outer join only works if the condition is the join of the two tables with "on".  
e.g. select * from a outer join b on a.field1=b.field2  
Right outer join are not implemented

### ResulSetMetaData

For ResultSetMetaData to be usable, the schema must be stored with the collection.  
If the schemas are stored, SQL tools can also be used. This was tested with SQL Workbench/J, JasperReport -Server / -Studio, Jetbrain DataGrid.

In the util directory there is an import routine for Typescript interface classes available.  
The file baseInterface contains the base interfaces and a data type definition for integer.  
In the importModel file, the database connection parameters must be filled in at the beginning and possibly the directory for the models.

### AQL commands with parameters

The parameter names must be numbered consecutively, starting with 1, and begin with a "@p".  
e.g. FOR c IN Country FILTER c.region==@p1 && c.isoCode2==@p2 RETURN c

### The following are not implemented

- SQL - DDL commands
- BLOB, CLOB, Streams, RowID, SQLXML, URL
- ResultSet - update functions
- Transaction

### Driver options

- jdbcUrl: jdbc:hcbraun:arangodb:[host]:[port]/[databaseName][;options=value]
- host: localhost
- port: 8529
- databaseName: _system
- options:
  - collectionAlias=[collection]:[alias]  // e.g. collectionAlias=Order:aOrder
  - arrayCollectionEnabled=[true|false]
  - arraySimpleValueEnabled=[true|false]
  - modifySqlStatement=[classname with package]
  - modifyAql=[classname with package]

#### Option collectionAlias

The collectionAlias option allows to use a different name for a collection than the collection name.  
e.g. collectionAlias=Order:aOrder

#### Option arrayCollectionEnabled

If the arrayCollectionEnabled option is set to true. Virtual collections are added for all object array properties in the main level of a collection.  
  
#### Option arraySimpleValueEnabled

If the arraySimpleValueEnabled option is set to true. Virtual collections are added for all simple array properties in the main level of a collection.  
  
#### Option modifySqlStatement

The referenced class must implement the IModifySQLStatement interface. This allows the SQL command to be adapted before it is rewritten as an AQL command.
This interface has only one method: String modifySQLStatement(String sqlStatement).

#### Option modifyAql

The referenced class must implement the IModifyAQL interface. This allows the AQL command to be adapted before it is executed.
This interface has only one method: String modifyAQL(String aqlCommand).

### Contribute or report incorrect queries

If SQL commands are implemented incorrectly, there are two options available.

1. Create issue with the SQL command that is not implemented correctly.
2. Or create a fork, with a test for the SQL command, implement it and then set a pull request. All existing tests must also be successful after the implementation.


### Developer: required for tests

A test database is needed for the tests, as well as a database user.

- database name: TestJDBC
- database user: testJdbc
- user password: pwd4Test&jdbc

The database user testJdbc needs admin rights for the TestJDBC database.