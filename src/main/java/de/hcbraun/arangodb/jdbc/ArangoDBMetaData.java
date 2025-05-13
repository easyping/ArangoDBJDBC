package de.hcbraun.arangodb.jdbc;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.IndexEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class ArangoDBMetaData implements DatabaseMetaData {

  private final Logger logger = LoggerFactory.getLogger(ArangoDBMetaData.class);
  private String schema = "adbdbo";
  private String separatorStructColumn = null;

  ArangoDBConnection con;

  public ArangoDBMetaData(ArangoDBConnection con, String schema, String separatorStructColumn) {
    this.con = con;
    this.schema = schema;
    this.separatorStructColumn = separatorStructColumn == null ? "." : separatorStructColumn;
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    logger.debug("allProceduresAreCallable");
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    logger.debug("allTablesAreSelectable");
    return false;
  }

  @Override
  public String getURL() throws SQLException {
    logger.debug("getURL");
    return null;
  }

  @Override
  public String getUserName() throws SQLException {
    logger.debug("getUserName");
    return con.getUserName();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    logger.debug("isReadOnly");
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    logger.debug("nullsAreSortedHigh");
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    logger.debug("nullsAreSortedLow");
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    logger.debug("nullsAreSortedAtStart"); // TODO überpürfen, ob Null Wert als erstes
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    logger.debug("nullsAreSortedAtEnd"); // TODO überpürfen, ob Null Wert als letztes
    return false;
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return "ArangoDB";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return con.getDatabase().getVersion().getVersion();
  }

  @Override
  public String getDriverName() throws SQLException {
    return "ArangoDB-JDBC";
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return getDriverMajorVersion() + "." + getDriverMinorVersion();
  }

  @Override
  public int getDriverMajorVersion() {
    return ArangoDBJDBCVersion.major;
  }

  @Override
  public int getDriverMinorVersion() {
    return ArangoDBJDBCVersion.minor;
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    logger.debug("usesLocalFiles");
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    logger.debug("usesLocalFilePerTable");
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    logger.debug("supportsMixedCaseIdentifiers");
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    logger.debug("storesUpperCaseIdentifiers");
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    logger.debug("storesLowerCaseIdentifiers");
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    logger.debug("storesMixedCaseIdentifiers");
    return true;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    logger.debug("supportsMixedCaseQuotedIdentifiers");
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    logger.debug("storesUpperCaseQuotedIdentifiers");
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    logger.debug("storesLowerCaseQuotedIdentifiers");
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    logger.debug("storesLowerCaseQuotedIdentifiers");
    return true;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    logger.debug("getIdentifierQuoteString");
    return " ";
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    logger.debug("getSQLKeywords");
    return null;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    logger.debug("getNumericFunctions");
    return null;
  }

  @Override
  public String getStringFunctions() throws SQLException {
    logger.debug("getStringFunctions");
    return null;
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    logger.debug("getSystemFunctions");
    return null;
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    logger.debug("getTimeDateFunctions");
    return null;
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    logger.debug("getSearchStringEscape");
    return null;
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    logger.debug("getExtraNameCharacters");
    return "";
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    logger.debug("supportsAlterTableWithAddColumn");
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    logger.debug("supportsAlterTableWithDropColumn");
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    logger.debug("supportsColumnAliasing");
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    logger.debug("nullPlusNonNullIsNull");
    return false;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    logger.debug("supportsConvert");
    return false;
  }

  @Override
  public boolean supportsConvert(int i, int i1) throws SQLException {
    logger.debug("supportsConvert");
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    logger.debug("supportsTableCorrelationNames");
    return false;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    logger.debug("supportsDifferentTableCorrelationNames");
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    logger.debug("supportsExpressionsInOrderBy");
    return false;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    logger.debug("supportsOrderByUnrelated");
    return true;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    logger.debug("supportsGroupBy");
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    logger.debug("supportsGroupByUnrelated");
    return false;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    logger.debug("supportsGroupByBeyondSelect");
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    logger.debug("supportsLikeEscapeClause");
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    logger.debug("supportsMultipleResultSets");
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    logger.debug("supportsMultipleTransactions");
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    logger.debug("supportsNonNullableColumns");
    return false;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    logger.debug("supportsMinimumSQLGrammar");
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    logger.debug("supportsCoreSQLGrammar");
    return false;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    logger.debug("supportsExtendedSQLGrammar");
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    logger.debug("supportsANSI92EntryLevelSQL");
    return true;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    logger.debug("supportsANSI92IntermediateSQL");
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    logger.debug("supportsANSI92FullSQL");
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    logger.debug("supportsIntegrityEnhancementFacility");
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    logger.debug("supportsOuterJoins");
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    logger.debug("supportsFullOuterJoins");
    return false;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    logger.debug("supportsLimitedOuterJoins");
    return false;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    logger.debug("getSchemaTerm");
    return null;
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    logger.debug("getProcedureTerm");
    return null;
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    logger.debug("getCatalogTerm");
    return null;
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    logger.debug("isCatalogAtStart");
    return false;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    logger.debug("getCatalogSeparator");
    return null;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    logger.debug("supportsSchemasInDataManipulation");
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    logger.debug("supportsSchemasInProcedureCalls");
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    logger.debug("supportsSchemasInTableDefinitions");
    return false;   // true
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    logger.debug("supportsSchemasInIndexDefinitions");
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    logger.debug("supportsSchemasInPrivilegeDefinitions");
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    logger.debug("supportsCatalogsInDataManipulation");
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    logger.debug("supportsCatalogsInProcedureCalls");
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    logger.debug("supportsCatalogsInTableDefinitions");
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    logger.debug("supportsCatalogsInIndexDefinitions");
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    logger.debug("supportsCatalogsInPrivilegeDefinitions");
    return false;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    logger.debug("supportsPositionedDelete");
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    logger.debug("supportsPositionedUpdate");
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    logger.debug("supportsSelectForUpdate");
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    logger.debug("supportsStoredProcedures");
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    logger.debug("supportsSubqueriesInComparisons");
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    logger.debug("supportsSubqueriesInExists");
    return false;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    logger.debug("supportsSubqueriesInIns");
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    logger.debug("supportsSubqueriesInQuantifieds");
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    logger.debug("supportsCorrelatedSubqueries");
    return false;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    logger.debug("supportsUnion");
    return false;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    logger.debug("supportsUnionAll");
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    logger.debug("supportsOpenCursorsAcrossCommit");
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    logger.debug("supportsOpenCursorsAcrossRollback");
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    logger.debug("supportsOpenStatementsAcrossCommit");
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    logger.debug("supportsOpenStatementsAcrossRollback");
    return false;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    logger.debug("getMaxBinaryLiteralLength");
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    logger.debug("getMaxCharLiteralLength");
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    logger.debug("getMaxColumnNameLength");
    return 0;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    logger.debug("getMaxColumnsInGroupBy");
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    logger.debug("getMaxColumnsInIndex");
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    logger.debug("getMaxColumnsInOrderBy");
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    logger.debug("getMaxColumnsInSelect");
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    logger.debug("getMaxColumnsInTable");
    return 0;
  }

  @Override
  public int getMaxConnections() throws SQLException {
    logger.debug("getMaxConnections");
    return 0;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    logger.debug("getMaxCursorNameLength");
    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    logger.debug("getMaxIndexLength");
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    logger.debug("getMaxSchemaNameLength");
    return 0;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    logger.debug("getMaxProcedureNameLength");
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    logger.debug("getMaxCatalogNameLength");
    return 0;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    logger.debug("getMaxRowSize");
    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    logger.debug("doesMaxRowSizeIncludeBlobs");
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    logger.debug("getMaxStatementLength");
    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException {
    logger.debug("getMaxStatements");
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    logger.debug("getMaxTableNameLength");
    return 0;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    logger.debug("getMaxTablesInSelect");
    return 0;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    logger.debug("getMaxUserNameLength");
    return 0;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    logger.debug("getDefaultTransactionIsolation");
    return 0;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    logger.debug("supportsTransactions");
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int i) throws SQLException {
    logger.debug("supportsTransactionIsolationLevel");
    return false;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    logger.debug("supportsDataDefinitionAndDataManipulationTransactions");
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    logger.debug("supportsDataManipulationTransactionsOnly");
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    logger.debug("dataDefinitionCausesTransactionCommit");
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    logger.debug("dataDefinitionIgnoredInTransactions");
    return false;
  }

  @Override
  public ResultSet getProcedures(String s, String s1, String s2) throws SQLException {
    logger.debug("getProcedures");
    return null;
  }

  @Override
  public ResultSet getProcedureColumns(String s, String s1, String s2, String s3) throws SQLException {
    logger.debug("getProcedureColumns");
    return null;
  }

  @Override
  public ResultSet getTables(String s, String s1, String s2, String[] strings) throws SQLException {
    logger.debug("getTables: " + s + " / " + s1 + " / " + s2 + " / " + Arrays.toString(strings));

    if (con != null) {
      Collection<CollectionEntity> cols = con.getDatabase().getCollections();
      ArrayList<String> lst = new ArrayList<>();
      for (CollectionEntity row : cols) {
        logger.debug("Table: " + row.getName());
        if (!row.getIsSystem())
          lst.add(row.getName());
      }
      if (con.getStructureManager().getVirtualCollections() != null) {
        lst.addAll(con.getStructureManager().getVirtualCollections().keySet());
      }
      lst.sort(Comparator.naturalOrder());
      return new ArangoDBCollectionResultSet(lst, schema, con);
    }

    return null;
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    logger.debug("getSchemas " + (schema != null ? schema : ""));
    ArrayList<HashMap<String, Object>> types = new ArrayList<>();
    if (schema != null) {
      HashMap<String, Object> s = new HashMap<>();
      s.put("TABLE_SCHEM", schema);
      s.put("TABLE_CATALOG", null);
      types.add(s);
    }
    return new ArangoDBListResultSet(types, new ArangoDBResultSetMetaData("// cols: TABLE_SCHEM:s,TABLE_CATALOG:s"));
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    logger.debug("getCatalogs");
    return new ArangoDBListResultSet(new ArrayList<>(), new ArangoDBResultSetMetaData("// cols: TABLE_CATALOG:s"));
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    logger.debug("getTableTypes");
    ArrayList<HashMap<String, Object>> types = new ArrayList<>();
    HashMap<String, Object> row = new HashMap<>();
    row.put("TABLE_TYPE", "TABLE");
    types.add(row);
    return new ArangoDBListResultSet(types, new ArangoDBResultSetMetaData("// cols: TABLE_TYPE:s"));
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    logger.debug("getColumns: " + catalog + " / " + schemaPattern + " / " + tableNamePattern + " / " + columnNamePattern);

    ArrayList<HashMap<String, Object>> cols = null;
    if ("%".equals(tableNamePattern)) {
      try {
        ArangoCursor<BaseDocument> cursor = con.getDatabase().query("FOR c IN COLLECTIONS() FILTER !STARTS_WITH(c.name, '_') RETURN {name: c.name, schema: SCHEMA_GET(c.name)}", BaseDocument.class);
        if (cursor != null) {
          cols = new ArrayList<>();
          while (cursor.hasNext()) {
            BaseDocument doc = cursor.next();
            Map<String, Object> sa = (Map) doc.getAttribute("schema");
            if (sa != null) {
              String tableName = (String) doc.getAttribute("name");
              logger.info("Table getColumns: " + tableName);
              CollectionSchema cSchema = con.getStructureManager().getSchema(tableName, new BaseDocument(sa));
              addSchemaColumns(cSchema, cSchema.getProperties(), "", cols, 0, tableName, schema, new ArrayList<String>());
            }
          }
        }
      } catch (ArangoDBException e) {
        e.printStackTrace();
      }
    } else {
      if (con != null && con.getStructureManager().getVirtualCollections().containsKey(tableNamePattern)) {
        SchemaVirtual sv = con.getStructureManager().getVirtualCollections().get(tableNamePattern);
        CollectionSchema cSchema = con.getStructureManager().getSchema(sv.getCollectionName());
        if (cSchema != null) {
          SchemaNode node = cSchema.getProperties().stream()
            .filter(n -> n.getName().equals(sv.getColumnName()))
            .findFirst()
            .orElse(null);
          if (node != null) {
            cols = new ArrayList<>();
            addSchemaRow(new SchemaNode("_key"), "", cols, 0, tableNamePattern, schema, Types.STRUCT);
            for (String ref : node.getReferences()) {
              SchemaReference sRef = cSchema.getReferences().get(ref);
              if (sRef != null) {
                addSchemaColumns(cSchema, sRef.getProperties(), "", cols, 1, tableNamePattern, schema, new ArrayList<String>());
              }
            }
          }
        }
      } else {
        CollectionSchema cSchema = con.getStructureManager().getSchema(tableNamePattern);
        cols = new ArrayList<>();
        if (cSchema != null) {
          addSchemaColumns(cSchema, cSchema.getProperties(), "", cols, 0, tableNamePattern, schema, new ArrayList<String>());
          if (columnNamePattern != null && !columnNamePattern.isEmpty()) {
            ArrayList<HashMap<String, Object>> nCols = new ArrayList<>();
            for (HashMap<String, Object> row : cols) {
              String colName = (String) row.get("COLUMN_NAME");
              if (colName.matches(columnNamePattern))
                nCols.add(row);
            }
            cols = nCols;
          }
        }
      }
    }
    if (cols != null) {
      return new ArangoDBListResultSet(cols, new ArangoDBResultSetMetaData("// cols: TABLE_CAT:s,TABLE_SCHEM:s," +
        "TABLE_NAME:s,COLUMN_NAME:s,DATA_TYPE:i,TYPE_NAME:s,COLUMN_SIZE:i,BUFFER_LENGTH:s,DECIMAL_DIGITS:i,NUM_PREC_RADIX:i," +
        "NULLABLE:i,REMARKS:s,COLUMN_DEF:s,SQL_DATA_TYPE:i,SQL_DATETIME_SUB:i,CHAR_OCTET_LENGTH:i,ORDINAL_POSITION:i," +
        "IS_NULLABLE:s,SCOPE_CATALOG:s,SCOPE_SCHEMA:s,SCOPE_TABLE:s,SOURCE_DATA_TYPE:i,IS_AUTOINCREMENT:s,IS_GENERATEDCOLUMN:s"));
    }
    return null;
  }

  private int addSchemaColumns(CollectionSchema cSchema, List<SchemaNode> properties, String prefix, ArrayList<HashMap<String, Object>> cols, int colPos, String tableName, String schema, List<String> lstCalledReferences) {
    for (SchemaNode node : properties) {
      if (node.getDataType().get(0) == Types.STRUCT &&
        ((node.getProperties() != null && !node.getProperties().isEmpty()) || (node.getReferences() != null && !node.getReferences().isEmpty()))) {
        if (node.getReferences() != null && !node.getReferences().isEmpty()) {
          for (String ref : node.getReferences()) {
            SchemaReference sRef = cSchema.getReferences() != null ? cSchema.getReferences().get(ref) : null;
            if (sRef != null && !lstCalledReferences.contains(sRef.getName())) {
              lstCalledReferences.add(sRef.getName());
              colPos = addSchemaColumns(cSchema, sRef.getProperties(), prefix + node.getName() + separatorStructColumn, cols, colPos, tableName, schema, lstCalledReferences);
              lstCalledReferences.remove(lstCalledReferences.size() - 1);
            }
            else {
              SchemaDatatype sDt = cSchema.getDatatypes() != null ? cSchema.getDatatypes().get(ref) : null;
              if (sDt != null) {
                colPos = addSchemaRow(node, prefix, cols, colPos, tableName, schema, sDt.getType());
              }
            }
          }
        } else
          colPos = addSchemaColumns(cSchema, node.getProperties(), prefix + node.getName() + separatorStructColumn, cols, colPos, tableName, schema, lstCalledReferences);
      } else {
        colPos = addSchemaRow(node, prefix, cols, colPos, tableName, schema, node.getDataType().get(0));
      }
    }
    return colPos;
  }

  private int addSchemaRow(SchemaNode node, String prefix, ArrayList<HashMap<String, Object>> cols, int colPos, String tableName, String schema, int dataType) {
    String colName = prefix + node.getName();
    if (cols.stream().noneMatch(c -> c.get("COLUMN_NAME").equals(colName))) {
      ++colPos;
      HashMap<String, Object> row = new HashMap<>();
      row.put("TABLE_CAT", null);
      row.put("TABLE_SCHEM", schema);
      row.put("TABLE_NAME", tableName);
      row.put("COLUMN_NAME", colName);
      row.put("DATA_TYPE", dataType);
      row.put("TYPE_NAME", "");
      row.put("COLUMN_SIZE", 0);
      row.put("BUFFER_LENGTH", null);
      row.put("DECIMAL_DIGITS", 0);
      row.put("NUM_PREC_RADIX", 10);
      row.put("NULLABLE", node.isNullable() ? columnNullable : columnNullableUnknown);
      row.put("REMARKS", node.getEnumValues() != null ? node.getEnumValues().toString() : "");
      row.put("COLUMN_DEF", null);
      row.put("SQL_DATA_TYPE", 0);
      row.put("SQL_DATETIME_SUB", 0);
      row.put("CHAR_OCTET_LENGTH", 0);
      row.put("ORDINAL_POSITION", colPos);
      row.put("IS_NULLABLE", "");
      row.put("SCOPE_CATALOG", null);
      row.put("SCOPE_SCHEMA", null);
      row.put("SCOPE_TABLE", null);
      row.put("SOURCE_DATA_TYPE", null);
      row.put("IS_AUTOINCREMENT", "");
      row.put("IS_GENERATEDCOLUMN", "");
      cols.add(row);
    }
    return colPos;
  }

  @Override
  public ResultSet getColumnPrivileges(String s, String s1, String s2, String s3) throws SQLException {
    logger.debug("getColumnPrivileges");
    return new ArangoDBListResultSet(new ArrayList<HashMap<String, Object>>(), new ArangoDBResultSetMetaData("// cols: TABLE_CAT:s," +
      "TABLE_SCHEM:s,TABLE_NAME:s,GRANTOR:s,GRANTEE:s,PRIVILEGE:s,IS_GRANTABLE:s"));
  }

  @Override
  public ResultSet getTablePrivileges(String s, String s1, String s2) throws SQLException {
    logger.debug("getTablePrivileges");
    return new ArangoDBListResultSet(new ArrayList<HashMap<String, Object>>(), new ArangoDBResultSetMetaData("// cols: TABLE_CAT:s," +
      "TABLE_SCHEM:s,TABLE_NAME:s,GRANTOR:s,GRANTEE:s,PRIVILEGE:s,IS_GRANTABLE:s"));
  }

  @Override
  public ResultSet getBestRowIdentifier(String s, String s1, String s2, int i, boolean b) throws SQLException {
    logger.debug("getBestRowIdentifier");
    return new ArangoDBListResultSet(new ArrayList<HashMap<String, Object>>(), new ArangoDBResultSetMetaData("// cols: SCOPE:i,COLUMN_NAME:s," +
      "DATA_TYPE:i,TYPE_NAME:s,COLUMN_SIZE:i,BUFFER_LENGTH:i,DECIMAL_DIGITS:i,PSEUDO_COLUMN:i"));
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
    logger.debug("getVersionColumns");
    ArrayList<HashMap<String, Object>> cols = new ArrayList<>();
    HashMap<String, Object> row = new HashMap<>();
    row.put("COLUMN_NAME", "_rev");
    row.put("DATA_TYPE", Types.VARCHAR);
    row.put("TYPE_NAME", "VARCHAR");
    row.put("COLUMN_SIZE", 50);
    row.put("PSEUDO_COLUMN", versionColumnPseudo);
    cols.add(row);
    return new ArangoDBListResultSet(cols, new ArangoDBResultSetMetaData("// cols: SCOPE:i,COLUMN_NAME:s," +
      "DATA_TYPE:i,TYPE_NAME:s,COLUMN_SIZE:i,BUFFER_LENGTH:i,DECIMAL_DIGITS:i,PSEUDO_COLUMN:i"));
  }

  @Override
  public ResultSet getPrimaryKeys(String queryCatalog, String querySchema, String queryTable) throws SQLException {
    logger.debug("getPrimaryKeys: " + queryCatalog + " / " + querySchema + " / " + queryTable);
    ArrayList<HashMap<String, Object>> cols = new ArrayList<>();
    HashMap<String, Object> row = new HashMap<>();
    row.put("TABLE_CAT", null);
    row.put("TABLE_SCHEM", schema);
    row.put("TABLE_NAME", queryTable);
    row.put("COLUMN_NAME", "_key");
    row.put("KEY_SEQ", 1);
    row.put("PK_NAME", "primary");
    cols.add(row);
    return new ArangoDBListResultSet(cols, new ArangoDBResultSetMetaData("// cols: TABLE_CAT:s,TABLE_SCHEM:s,TABLE_NAME:s,KEY_SEQ:i,PK_NAME:s"));
  }

  @Override
  public ResultSet getImportedKeys(String s, String s1, String s2) throws SQLException {
    logger.debug("getImportedKeys");
    return new ArangoDBListResultSet(new ArrayList<HashMap<String, Object>>(), new ArangoDBResultSetMetaData(
      "// cols: PKTABLE_CAT:s,PKTABLE_SCHEM:s,PKTABLE_NAME:s,PKCOLUMN_NAME:s,FKTABLE_CAT:s,FKTABLE_SCHEM:s," +
        "FKTABLE_NAME:s,FKCOLUMN_NAME:s,KEY_SEQ:s,UPDATE_RULE:i,DELETE_RULE:i,FK_NAME:s,PK_NAME:s,DEFERRABILITY:i"));
  }

  @Override
  public ResultSet getExportedKeys(String s, String s1, String s2) throws SQLException {
    logger.debug("getExportedKeys");
    return new ArangoDBListResultSet(new ArrayList<HashMap<String, Object>>(), new ArangoDBResultSetMetaData(
      "// cols: PKTABLE_CAT:s,PKTABLE_SCHEM:s,PKTABLE_NAME:s,PKCOLUMN_NAME:s,FKTABLE_CAT:s,FKTABLE_SCHEM:s," +
        "FKTABLE_NAME:s,FKCOLUMN_NAME:s,KEY_SEQ:s,UPDATE_RULE:i,DELETE_RULE:i,FK_NAME:s,PK_NAME:s,DEFERRABILITY:i"));
  }

  @Override
  public ResultSet getCrossReference(String s, String s1, String s2, String s3, String s4, String s5) throws SQLException {
    logger.debug("getCrossReference");
    return new ArangoDBListResultSet(new ArrayList<HashMap<String, Object>>(), new ArangoDBResultSetMetaData(
      "// cols: PKTABLE_CAT:s,PKTABLE_SCHEM:s,PKTABLE_NAME:s,PKCOLUMN_NAME:s,FKTABLE_CAT:s,FKTABLE_SCHEM:s," +
        "FKTABLE_NAME:s,FKCOLUMN_NAME:s,KEY_SEQ:s,UPDATE_RULE:i,DELETE_RULE:i,FK_NAME:s,PK_NAME:s,DEFERRABILITY:i"));
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    logger.debug("getTypeInfo");
    ArrayList<HashMap<String, Object>> types = new ArrayList<>();
    HashMap<String, Object> row = new HashMap<>();
    row.put("TYPE_NAME", "VARCHAR");
    row.put("DATA_TYPE", Types.VARCHAR);
    row.put("NULLABLE", typeNullable);
    row.put("CASE_SENSITIVE", true);
    types.add(row);
    row = new HashMap<>();
    row.put("TYPE_NAME", "DOUBLE");
    row.put("DATA_TYPE", Types.DOUBLE);
    row.put("NULLABLE", typeNullable);
    types.add(row);
    row = new HashMap<>();
    row.put("TYPE_NAME", "INTEGER");
    row.put("DATA_TYPE", Types.INTEGER);
    row.put("NULLABLE", typeNullable);
    types.add(row);
    row = new HashMap<>();
    row.put("TYPE_NAME", "DATE");
    row.put("DATA_TYPE", Types.DATE);
    row.put("NULLABLE", typeNullable);
    types.add(row);
    row = new HashMap<>();
    row.put("TYPE_NAME", "TIME");
    row.put("DATA_TYPE", Types.TIME);
    row.put("NULLABLE", typeNullable);
    types.add(row);
    row = new HashMap<>();
    row.put("TYPE_NAME", "TIMESTAMP");
    row.put("DATA_TYPE", Types.TIMESTAMP);
    row.put("NULLABLE", typeNullable);
    types.add(row);
    row = new HashMap<>();
    row.put("TYPE_NAME", "BOOLEAN");
    row.put("DATA_TYPE", Types.BOOLEAN);
    row.put("NULLABLE", typeNullable);
    types.add(row);
    row = new HashMap<>();
    row.put("TYPE_NAME", "ARRAY");
    row.put("DATA_TYPE", Types.ARRAY);
    row.put("NULLABLE", typeNullable);
    types.add(row);
    return new ArangoDBListResultSet(types, new ArangoDBResultSetMetaData(
      "// cols: TYPE_NAME:s,DATA_TYPE:i,PRECISION:i,LITERAL_PREFIX:s,LITERAL_SUFFIX:s,CREATE_PARAMS:s," +
        "NULLABLE:i,CASE_SENSITIVE:b,SEARCHABLE:i,UNSIGNED_ATTRIBUTE:b,FIXED_PREC_SCALE:b,AUTO_INCREMENT:b," +
        "LOCAL_TYPE_NAME:s,MINIMUM_SCALE:i,MAXIMUM_SCALE:i,SQL_DATA_TYPE:i,SQL_DATETIME_SUB:i,NUM_PREC_RADIX:i"));
  }

  //    Parameters:
  //    catalog - a catalog name; must match the catalog name as it is stored in this database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
  //    schema - a schema name; must match the schema name as it is stored in this database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
  //    table - a table name; must match the table name as it is stored in this database
  //    unique - when true, return only indices for unique values; when false, return indices regardless of whether unique or not
  //    approximate - when true, result is allowed to reflect approximate or out of data values; when false, results are requested to be accurate
  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
    logger.debug("getIndexInfo");
    ArangoCollection col = con.getDatabase().collection(con.getAliasCollection(table));
    Collection<IndexEntity> indexes = col.getIndexes();

    ArrayList<HashMap<String, Object>> idx = new ArrayList<>();
    for (IndexEntity index : indexes) {
      if (unique && !index.getUnique())
        continue;
      int pos = 1;
      for (String field : index.getFields()) {
        HashMap<String, Object> row = new HashMap<>();
        row.put("TABLE_CAT", null);
        row.put("TABLE_SCHEM", schema);
        row.put("TABLE_NAME", table);
        row.put("NON_UNIQUE", !index.getUnique());
        row.put("INDEX_QUALIFIER", null);
        row.put("INDEX_NAME", index.getName());
        row.put("TYPE", 0);
        row.put("ORDINAL_POSITION", pos++);
        row.put("COLUMN_NAME", field);
        row.put("ASC_OR_DESC", "A");
        row.put("CARDINALITY", 0);
        row.put("PAGES", 0);
        row.put("FILTER_CONDITION", null);
        idx.add(row);
      }
    }

//    Retrieves a description of the given table's indices and statistics. They are ordered by NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
//    Each index column description has the following columns:
//
//    TABLE_CAT String => table catalog (may be null)
//    TABLE_SCHEM String => table schema (may be null)
//    TABLE_NAME String => table name
//    NON_UNIQUE boolean => Can index values be non-unique. false when TYPE is tableIndexStatistic
//    INDEX_QUALIFIER String => index catalog (may be null); null when TYPE is tableIndexStatistic
//    INDEX_NAME String => index name; null when TYPE is tableIndexStatistic
//    TYPE short => index type:
//      tableIndexStatistic - this identifies table statistics that are returned in conjuction with a table's index descriptions
//      tableIndexClustered - this is a clustered index
//      tableIndexHashed - this is a hashed index
//      tableIndexOther - this is some other style of index
//    ORDINAL_POSITION short => column sequence number within index; zero when TYPE is tableIndexStatistic
//    COLUMN_NAME String => column name; null when TYPE is tableIndexStatistic
//    ASC_OR_DESC String => column sort sequence, "A" => ascending, "D" => descending, may be null if sort sequence is not supported; null when TYPE is tableIndexStatistic
//    CARDINALITY int => When TYPE is tableIndexStatistic, then this is the number of rows in the table; otherwise, it is the number of unique values in the index.
//            PAGES int => When TYPE is tableIndexStatisic then this is the number of pages used for the table, otherwise it is the number of pages used for the current index.
//            FILTER_CONDITION String => Filter condition, if any. (may be null)
    return new ArangoDBListResultSet(idx, new ArangoDBResultSetMetaData("// cols: TABLE_CAT:s,TABLE_SCHEM:s," +
      "TABLE_NAME:s,NON_UNIQUE:b,INDEX_QUALIFIER:s,INDEX_NAME:s,TYPE:i,ORDINAL_POSITION:i,COLUMN_NAME:s," +
      "ASC_OR_DESC:s,CARDINALITY:i,PAGES:i,FILTER_CONDITION:s"));
  }

  @Override
  public boolean supportsResultSetType(int i) throws SQLException {
    logger.debug("supportsResultSetType");
    return false;
  }

  @Override
  public boolean supportsResultSetConcurrency(int i, int i1) throws SQLException {
    logger.debug("supportsResultSetConcurrency");
    return false;
  }

  @Override
  public boolean ownUpdatesAreVisible(int i) throws SQLException {
    logger.debug("ownUpdatesAreVisible");
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int i) throws SQLException {
    logger.debug("ownDeletesAreVisible");
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int i) throws SQLException {
    logger.debug("ownInsertsAreVisible");
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int i) throws SQLException {
    logger.debug("othersUpdatesAreVisible");
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int i) throws SQLException {
    logger.debug("othersDeletesAreVisible");
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int i) throws SQLException {
    logger.debug("othersInsertsAreVisible");
    return false;
  }

  @Override
  public boolean updatesAreDetected(int i) throws SQLException {
    logger.debug("updatesAreDetected");
    return false;
  }

  @Override
  public boolean deletesAreDetected(int i) throws SQLException {
    logger.debug("deletesAreDetected");
    return false;
  }

  @Override
  public boolean insertsAreDetected(int i) throws SQLException {
    logger.debug("insertsAreDetected");
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    logger.debug("supportsBatchUpdates");
    return false;
  }

  @Override
  public ResultSet getUDTs(String s, String s1, String s2, int[] ints) throws SQLException {
    logger.debug("getUDTs");
    return null;
  }

  @Override
  public Connection getConnection() throws SQLException {
    logger.debug("getConnection");
    return con;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    logger.debug("supportsSavepoints");
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    logger.debug("supportsNamedParameters");
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    logger.debug("supportsMultipleOpenResults");
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    logger.debug("supportsGetGeneratedKeys");
    return false;
  }

  @Override
  public ResultSet getSuperTypes(String s, String s1, String s2) throws SQLException {
    logger.debug("getSuperTypes");
    return null;
  }

  @Override
  public ResultSet getSuperTables(String s, String s1, String s2) throws SQLException {
    logger.debug("getSuperTables");
    return null;
  }

  @Override
  public ResultSet getAttributes(String s, String s1, String s2, String s3) throws SQLException {
    logger.debug("getAttributes");
    return null;
  }

  @Override
  public boolean supportsResultSetHoldability(int i) throws SQLException {
    logger.debug("supportsResultSetHoldability");
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    logger.debug("getResultSetHoldability");
    return 0;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    logger.debug("getDatabaseMajorVersion");
    String[] v = con.getDatabase().getVersion().getVersion().split("\\.");
    return Integer.parseInt(v[0]);
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    logger.debug("getDatabaseMinorVersion");
    String[] v = con.getDatabase().getVersion().getVersion().split("\\.");
    return Integer.parseInt(v[1]);
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    logger.debug("getJDBCMajorVersion");
    return 0;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    logger.debug("getJDBCMinorVersion");
    return 0;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    logger.debug("getSQLStateType");
    return 0;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    logger.debug("locatorsUpdateCopy");
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    logger.debug("supportsStatementPooling");
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    logger.debug("getRowIdLifetime");
    return null;
  }

  @Override
  public ResultSet getSchemas(String s, String s1) throws SQLException {
    logger.debug("getSchemas");
    return new ArangoDBListResultSet(new ArrayList<>());
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    logger.debug("supportsStoredFunctionsUsingCallSyntax");
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    logger.debug("autoCommitFailureClosesAllResultSets");
    return false;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    logger.debug("getClientInfoProperties");
    return null;
  }

  @Override
  public ResultSet getFunctions(String s, String s1, String s2) throws SQLException {
    logger.debug("getFunctions");
    return null;
  }

  @Override
  public ResultSet getFunctionColumns(String s, String s1, String s2, String s3) throws SQLException {
    logger.debug("getFunctionColumns");
    return null;
  }

  @Override
  public ResultSet getPseudoColumns(String s, String s1, String s2, String s3) throws SQLException {
    logger.debug("getPseudoColumns");
    return null;
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    logger.debug("generatedKeyAlwaysReturned");
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> aClass) throws SQLException {
    logger.debug("unwrap");
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    logger.debug("isWrapperFor");
    return false;
  }
}
