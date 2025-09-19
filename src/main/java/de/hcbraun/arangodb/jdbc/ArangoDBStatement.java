package de.hcbraun.arangodb.jdbc;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.AqlParseEntity;
import com.arangodb.entity.BaseDocument;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class AQLFunction {
  String name;
  int[] parameterOrder;

  AQLFunction(String name, int[] parameterOrder) {
    this.name = name;
    this.parameterOrder = parameterOrder;
  }
}

public class ArangoDBStatement implements Statement {

  private final Logger logger = LoggerFactory.getLogger(ArangoDBStatement.class);

  private final static Pattern commentsPattern = Pattern.compile("(//.*?$)|(/\\*.*?\\*/)", Pattern.MULTILINE | Pattern.DOTALL);
  private final static Pattern patSql = Pattern.compile("select\\s+(.*)\\s+from\\s+(.*?)\\s*(where\\s(.*?)\\s*)?(group by\\s(.*?)\\s*)?(order by\\s(.*?)\\s*)?;", Pattern.CASE_INSENSITIVE);
  private final static Pattern patWhCond = Pattern.compile("( and | or |\\(|\\))", Pattern.CASE_INSENSITIVE);
  private final static Pattern patWhOp = Pattern.compile("(>=|<=|<>|!=|=|>|<|not like|like)", Pattern.CASE_INSENSITIVE);

  protected ArangoDBConnection connection;
  protected ArangoDatabase database = null;

  private int maxRows = 0;
  private String separatorStructColumn = null;

  private final static List<String> aggregateSqlFunc = Arrays.asList("LEN", "AVG", "COUNT", "MAX", "MIN", "SUM");
  private final static List<String> lstBoolValue = Arrays.asList("0", "1");

  Map<String, AQLFunction> mapSQLFuncToAQLFunc = Stream.of(new Object[][]{
    {"CHAR", new AQLFunction("TO_CHAR", null)},
    {"CONCAT_WS", new AQLFunction("CONCAT_SEPARATOR", null)},
    {"LEN", new AQLFunction("LENGTH", null)},
    {"REPLACE", new AQLFunction("REGEX_REPLACE", null)},
    {"REPLICATE", new AQLFunction("REPEAT", null)},
    {"ATN2", new AQLFunction("ATAN2", null)},
    {"CEILING", new AQLFunction("CEIL", null)},
    {"POWER", new AQLFunction("POW", null)},
    {"DATEADD", new AQLFunction("DATE_ADD", new int[]{3, 2, 1})},
    {"DATEDIFF", new AQLFunction("DATE_ADD", new int[]{3, 1, 2})},
    {"DATEFORMPARTS", new AQLFunction("DATE_TIMESTAMP", null)},
    {"DAY", new AQLFunction("DATE_DAY", null)},
    {"GETDATE", new AQLFunction("DATE_NOW", null)},
    {"GETUTCDATE", new AQLFunction("DATE_ISO8601", null)},
    {"ISDATE", new AQLFunction("IS_DATESTRING", null)},
    {"MONTH", new AQLFunction("DATE_MONTH", null)},
    {"SYSDATETIME", new AQLFunction("DATE_NOW", null)},
    {"YEAR", new AQLFunction("DATE_YEAR", null)},
    {"ISNULL", new AQLFunction("IS_NULL", new int[]{-1, 1})},
    {"ISNUMERIC", new AQLFunction("IS_NUMBER", null)}
  }).collect(Collectors.toMap(data -> (String) data[0], data -> (AQLFunction) data[1]));

  private static class AppendOption {
    int aggregateNo = 0, collectionNo = 1;
    StringBuilder aggregate = null;
    String sqlAlias = null, aqlAlias = null;
    HashMap<String, String> additionalLstTabAlias = null;
  }

  protected ArangoDBStatement(ArangoDBConnection connection) {
    this.connection = connection;
    if (connection != null)
      database = connection.getDatabase();
  }

  protected ArangoDBStatement(ArangoDBConnection connection, String separatorStructColumn) {
    this.connection = connection;
    if (connection != null)
      database = connection.getDatabase();
    if (separatorStructColumn != null && !separatorStructColumn.isEmpty() && !".".equals(separatorStructColumn))
      this.separatorStructColumn = separatorStructColumn;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("unwrap");
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("isWrapperFor");
    return false;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    logger.debug("executeQuery " + sql);
    try {
      QueryInfo qi = getAQL(sql, null);
      return new ArangoDBResultSet(database.query(qi.aql, BaseDocument.class), this, qi.rsmd);
    } catch (ArangoDBException e) {
      e.printStackTrace();
      throw new SQLException(e.getErrorMessage());
    }
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    logger.debug("executeUpdate " + sql);
    ResultSet rs = executeQuery(sql);
    int c = 0;
    if (rs != null) {
      while (rs.next())
        c++;
      rs.close();
    }
    return c;
  }

  @Override
  public void close() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("close");
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("getMaxFieldSize");
    return 0;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("setMaxFieldSize " + max);
  }

  @Override
  public int getMaxRows() throws SQLException {
    logger.debug("getMaxRows");
    return maxRows;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    logger.debug("setMaxRows " + max);
    maxRows = Math.max(max, 0);
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("setEscapeProcessing " + enable);
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("getQueryTimeout");
    return 0;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("setQueryTimeout " + seconds);
  }

  @Override
  public void cancel() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("cancel");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("getWarnings");
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("clearWarnings");
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("setCursorName " + name);
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    logger.debug("execute " + sql);
    return executeQuery(sql) != null;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("getResultSet");
    return null;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("getUpdateCount");
    return -1;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("getMoreResults");
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("setFetchDirection");
  }

  @Override
  public int getFetchDirection() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("getFetchDirection");
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("setFetchSize");
  }

  @Override
  public int getFetchSize() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("getFetchSize");
    return 0;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("getResultSetConcurrency");
    return 0;
  }

  @Override
  public int getResultSetType() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("getResultSetType");
    return 0;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("addBatch " + sql);
  }

  @Override
  public void clearBatch() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("clearBatch");
  }

  @Override
  public int[] executeBatch() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("executeBatch");
    return null;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("getMoreResults");
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("getGeneratedKeys");
    return null;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("executeUpdate " + sql + " / " + autoGeneratedKeys);
    return 0;
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("executeUpdate " + sql + " / " + Arrays.toString(columnIndexes));
    return 0;
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("executeUpdate " + sql + " / " + Arrays.toString(columnNames));
    return 0;
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("execute " + sql + " / " + autoGeneratedKeys);
    return false;
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("execute " + sql + " / " + Arrays.toString(columnIndexes));
    return false;
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("execute" + sql + " / " + Arrays.toString(columnNames));
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("getResultSetHoldability");
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("isClosed");
    return false;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("setPoolable");
  }

  @Override
  public boolean isPoolable() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("isPoolable");
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("closeOnCompletion");
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("isCloseOnCompletion");
    return false;
  }

  protected String modifySQLBeforeExecute(String sql) {
    if (connection != null && connection.getModifySqlStatement() != null) {
      sql = connection.getModifySqlStatement().modifySQLBeforeExecute(sql);
      logger.debug("modify sql: {}", sql);
    }
    return sql;
  }

  protected String modifyAQLBeforeExecute(String aql) {
    if (connection != null && connection.getModifyAql() != null) {
      aql = connection.getModifyAql().modifyAQLBeforeExecute(aql);
      logger.debug("modify aql: {}", aql);
    }
    return aql;
  }

  protected QueryInfo getAQL(String sql, Map<String, Object> parameters) {
    StringBuilder comments = null, newSql = null;
    sql = modifySQLBeforeExecute(sql);
    sql = sql.replaceAll("\r\n", " ").replaceAll("\n\r", " ").replaceAll("\n", " ").replaceAll("\t", " ");
    Matcher matcher = commentsPattern.matcher(sql);
    int sPos = 0;
    while (matcher.find()) {
      if (comments == null) {
        comments = new StringBuilder();
        newSql = new StringBuilder();
      }
      comments.append(matcher.group());
      newSql.append(sql.substring(sPos, matcher.start()));
      sPos = matcher.end();
    }
    if (sPos > 0)
      newSql.append(sql.substring(sPos));
    if (newSql != null) {
      sql = newSql.toString();
      System.out.println(comments);
    }
    if (this.connection != null) {
      try {
        String s = this.connection.getSchema();
        sql = sql.replaceAll("(( |,)" + s + "\\.)", " ");
      } catch (SQLException e) {
      }
    }

    StructureManager sm = connection != null ? connection.getStructureManager() : null;
    QueryInfo qi = new QueryInfo();
    qi.parameters = parameters;
    boolean sqlSelect = false, aqlQuery = false;
    ArrayList<ColInfo> lstRCols = null;
    try {
      net.sf.jsqlparser.statement.Statement parseStat = CCJSqlParserUtil.parse(sql);
      AppendOption appendOpt = new AppendOption();
      if (parseStat instanceof Select) {
        sqlSelect = true;
        Select select = (Select) parseStat;
        if (select.getSelectBody() instanceof PlainSelect) {
          PlainSelect plain = (PlainSelect) select.getSelectBody();

          HashMap<String, String> lstTabAlias = new HashMap<>();
          lstRCols = new ArrayList<>();
          qi.aql = getPlainSelect(plain, lstTabAlias, lstRCols, appendOpt);
        }
      } else if (parseStat instanceof Insert) {
        Insert insert = (Insert) parseStat;
        StringBuilder sb;
        ExpressionList<?> lstPara = null;
        List<SelectItem<?>> lstSelect = null;
        if (insert.getSelect() instanceof PlainSelect) {
          String select = getAQL(insert.getSelect().toString(), parameters).aql;
          if (select != null) {
            sb = new StringBuilder(select.substring(0, select.indexOf(" RETURN {")));
            sb.append(" ");
          } else
            sb = new StringBuilder();
          lstSelect = insert.getPlainSelect().getSelectItems();
        } else {
          sb = new StringBuilder();
          lstPara = insert.getValues().getExpressions();
        }
        sb.append("INSERT {");
        List<Column> lstCol = insert.getColumns();
        for (int i = 0; i < lstCol.size(); i++) {
          if (i > 0)
            sb.append(",");
          Column col = lstCol.get(i);
          sb.append(modifyColumnName(col)).append(":");
          if (lstPara != null) {
            Expression p = lstPara.get(i);
            if (p instanceof JdbcParameter)
              sb.append("@p").append(((JdbcParameter) p).getIndex());
            else
              sb.append(appendExpression(p, null, "c1", appendOpt, false, sm, null));
          } else if (lstSelect != null) {
            SelectItem si = lstSelect.get(i);
            sb.append(appendExpression(si.getExpression(), null, "c1", appendOpt, false, sm, null));
          }
        }
        sb.append("} INTO ").append(insert.getTable().getName());
        sb.append(" LET inserted = NEW RETURN inserted._key");
        qi.aql = sb.toString();
      } else if (parseStat instanceof Update) {
        Update update = (Update) parseStat;
        StringBuilder sb = new StringBuilder("FOR c1 IN ");
        sb.append(update.getTable().getName());
        if (update.getWhere() != null) {
          sb.append(" FILTER ");
          sb.append(appendExpression(update.getWhere(), null, "c1", appendOpt, false, sm, null));
        }
        sb.append(" UPDATE c1._key WITH {");
        for (UpdateSet us : update.getUpdateSets()) {
          List<Column> lstCol = us.getColumns();
          ExpressionList<?> lstPara = us.getValues();
          for (int i = 0; i < lstCol.size(); i++) {
            if (i > 0)
              sb.append(",");
            sb.append(getSqlColumn(lstCol.get(i), null, null, appendOpt, null));
            sb.append(":");
            sb.append(appendExpression(lstPara.get(i), null, "c1", appendOpt, false, sm, null));
          }
        }
        sb.append("} IN ").append(update.getTable().getName());
        sb.append(" LET updated = NEW RETURN updated._key");
        qi.aql = sb.toString();
      } else if (parseStat instanceof Delete) {
        Delete delete = (Delete) parseStat;
        StringBuilder sb = new StringBuilder("FOR c1 IN ");
        sb.append(delete.getTable().getName());
        if (delete.getWhere() != null) {
          sb.append(" FILTER ");
          sb.append(appendExpression(delete.getWhere(), null, "c1", appendOpt, false, sm, null));
        }
        sb.append(" REMOVE c1._key IN ").append(delete.getTable().getName());
        sb.append(" LET removed = OLD RETURN removed._key");
        qi.aql = sb.toString();
      }
    } catch (JSQLParserException e) {
      qi.aql = sql;
      aqlQuery = true;
      // Create list of columns metadata information
      if (database != null) {
        AqlParseEntity aPEntity = database.parseQuery(sql);
        // get schema information from all collections
        HashMap<String, HashMap<String, ColInfo>> lstColsDesc = readCollectionSchema(aPEntity.getCollections());
        // Search alias for collections
        HashMap<String, String> lstAliasCol = new HashMap<>();
        lstRCols = new ArrayList<>();
        for (AqlParseEntity.AstNode rootNode : aPEntity.getAst()) {
          for (AqlParseEntity.AstNode astNode : rootNode.getSubNodes()) {
            switch (astNode.getType()) {
              case "for":
                String v = null, c = null;
                for (AqlParseEntity.AstNode forNode : astNode.getSubNodes()) {
                  switch (forNode.getType()) {
                    case "variable":
                      v = forNode.getName();
                      break;
                    case "collection":
                      c = forNode.getName();
                      break;
                  }
                }
                if (v != null && c != null)
                  lstAliasCol.put(v, c);
                break;
              case "let":
                String vl = null, cl = null;
                for (AqlParseEntity.AstNode forNode : astNode.getSubNodes()) {
                  switch (forNode.getType()) {
                    case "variable":
                      vl = forNode.getName();
                      break;
                    case "function call":
                      for (AqlParseEntity.AstNode callNode : forNode.getSubNodes()) {
                        for (AqlParseEntity.AstNode arrayNode : callNode.getSubNodes()) {
                          if ("value".equals(arrayNode.getType()))
                            cl = (String) arrayNode.getValue();
                        }
                      }
                      break;
                  }
                }
                if (vl != null && cl != null)
                  lstAliasCol.put(vl, cl);
                break;
              case "return":
                for (AqlParseEntity.AstNode retNode : astNode.getSubNodes()) {
                  if ("object".equals(retNode.getType())) {
                    for (AqlParseEntity.AstNode objNode : retNode.getSubNodes()) {
                      ColInfo colInfo = null;
                      if ("object element".equals(objNode.getType())) {
                        for (AqlParseEntity.AstNode objEleNode : objNode.getSubNodes()) {
                          if ("attribute access".equals(objEleNode.getType())) {
                            String attColN = null;
                            for (AqlParseEntity.AstNode attNode : objEleNode.getSubNodes()) {
                              if ("reference".equals(attNode.getType())) {
                                attColN = lstAliasCol.get(attNode.getName());
                              }
                            }
                            if (attColN != null) {
                              HashMap<String, ColInfo> cI = lstColsDesc.get(attColN);
                              if (cI != null)
                                colInfo = cI.get(objEleNode.getName()); // Property in collection
                            }
                          }
                        }
                      }
                      if (colInfo != null) {
                        try {
                          ColInfo nCI = (ColInfo) colInfo.clone();
                          nCI.setName(objNode.getName()); // Property name in result
                          lstRCols.add(nCI);
                        } catch (CloneNotSupportedException ex) {
                          ex.printStackTrace();
                        }
                      }
                    }
                  }
                }
                break;
            }
          }
        }
        if (lstRCols.isEmpty())
          lstRCols = null;
      }
    }
    qi.aql = modifyAQLBeforeExecute(qi.aql);

    if (comments != null)
      qi.rsmd = new ArangoDBResultSetMetaData(comments.toString());
    else if (qi.aql != null && qi.aql.startsWith("RETURN LENGTH("))
      ;
    else if (qi.aql != null) {
      String q = qi.aql;
      if ((aqlQuery || sqlSelect) && !q.toLowerCase().contains("insert ") && !q.toLowerCase().contains("update ") &&
        !q.toLowerCase().contains("remove ")) {
        logger.debug("RSMD-aql: {}", q);
        if (lstRCols != null && !lstRCols.isEmpty()) {
          qi.rsmd = new ArangoDBResultSetMetaData(lstRCols);
        } else {
          String r = q.substring(q.toLowerCase().lastIndexOf(" return "));
          q = q.substring(0, q.toLowerCase().lastIndexOf(" return ")) + " LIMIT 1 " + r;
          if (database != null) {
            ArangoCursor<BaseDocument> cur = database.query(q, BaseDocument.class, parameters);
            if (cur.hasNext())
              qi.rsmd = new ArangoDBResultSetMetaData(cur.next());
            else
              qi.rsmd = null;
          }
        }
      } else
        qi.rsmd = null;
    }
    logger.debug("aql: {}", qi.aql);
    return qi;
  }

  private String getPlainSelect(PlainSelect plain, HashMap<String, String> lstTabAlias, ArrayList<ColInfo> lstRCols, AppendOption appendOpt) {
    StringBuilder sb = new StringBuilder("FOR ");
    String alias, dftAlias, dftTabName;
    Table fromItem = (Table) plain.getFromItem();
    if (fromItem.getAlias() != null && fromItem.getAlias().getName() != null &&
      !fromItem.getAlias().getName().equals(fromItem.getName()))
      alias = fromItem.getAlias().getName();
    else
      alias = "c" + (appendOpt.collectionNo++);
    dftAlias = alias;
    StructureManager sm = connection != null ? connection.getStructureManager() : null;
    SchemaVirtual sv = sm != null ? sm.getVirtualCollections().get(fromItem.getName()) : null;
    List<String> lstAliasOfSimpleReference = new ArrayList<>();
    if (sv != null) {
      String alias2 = "c" + (appendOpt.collectionNo++);
      sb.append(alias2).append(" IN ").append(sv.getCollectionName()).append(" FOR ").append(alias).append(" IN ").append(alias2).append(".").append(sv.getColumnName()).append(" || []");
      dftTabName = sv.getCollectionName();
      lstTabAlias.put(fromItem.getName(), alias);
      lstTabAlias.put(sv.getCollectionName(), alias2);
      if (sv.isSimpleReferences())
        lstAliasOfSimpleReference.add(alias2);
    } else {
      sb.append(alias).append(" IN ").append(getAliasCollection(fromItem.getName()));
      dftTabName = getAliasCollection(fromItem.getName());
      lstTabAlias.put(fromItem.getName(), alias);
    }

    if (plain.getJoins() != null && !plain.getJoins().isEmpty()) {
      for (int i = 0; i < plain.getJoins().size(); i++) {
        Join j = plain.getJoins().get(i);
        fromItem = (Table) j.getRightItem();
        sv = sm != null ? sm.getVirtualCollections().get(fromItem.getName()) : null;
        if (sv != null) {
          String tAlias = lstTabAlias.get(sv.getCollectionName());
          boolean addJoin = false;
          if (tAlias == null) {
            tAlias = "c" + (appendOpt.collectionNo++);
            lstTabAlias.put(sv.getCollectionName(), tAlias);
            sb.append(" FOR ").append(tAlias).append(" IN ").append(sv.getCollectionName());
            addJoin = true;
          }
          String vAlias = "c" + (appendOpt.collectionNo++);
          sb.append(" FOR ").append(vAlias).append(" IN ").append(tAlias).append(".").append(sv.getColumnName()).append(" || []");
          lstTabAlias.put(fromItem.getName(), vAlias);
          if (sv.isSimpleReferences())
            lstAliasOfSimpleReference.add(vAlias);
          if (addJoin && j.getOnExpressions() != null && !j.getOnExpressions().isEmpty()) {
            if (j.getOnExpressions().size() == 1) {
              Expression on = j.getOnExpressions().iterator().next();
              if (on instanceof EqualsTo || (on instanceof Parenthesis && ((Parenthesis) on).getExpression() instanceof EqualsTo)) {
                EqualsTo et = (EqualsTo) (on instanceof EqualsTo ? on : ((Parenthesis) on).getExpression());
                Column left = (Column) et.getLeftExpression();
                Column right = (Column) et.getRightExpression();
                sb.append(" FILTER ").append(lstTabAlias.get(left.getTable().getName())).append(".").append(left.getColumnName()).append("==").append(lstTabAlias.get(right.getTable().getName())).append(".").append(right.getColumnName());
              }
            }
          }
        } else if (j.getOnExpressions() != null && !j.getOnExpressions().isEmpty()) {
          // check are only ._key use in on expression
          if (j.getOnExpressions().size() == 1) {
            String filterPara = null;
            String aliasFrom = lstTabAlias.get(fromItem.getName());
            if (aliasFrom == null)
              aliasFrom = dftAlias;
            String s1 = fromItem.getName() + "._key", s2 = aliasFrom + "._key";
            String sI1 = fromItem.getName() + "._id", sId = null;
            for (Expression on : j.getOnExpressions()) {
              if (on instanceof EqualsTo) {
                EqualsTo et = (EqualsTo) on;
                if (et.getLeftExpression().toString().equals(s1) || et.getLeftExpression().toString().equals(s2))
                  filterPara = getSqlColumn((Column) et.getRightExpression(), lstTabAlias, dftAlias, appendOpt, lstAliasOfSimpleReference);
                else if (et.getRightExpression().toString().equals(s1) || et.getRightExpression().toString().equals(s2))
                  filterPara = getSqlColumn((Column) et.getLeftExpression(), lstTabAlias, dftAlias, appendOpt, lstAliasOfSimpleReference);
                else if (et.getLeftExpression().toString().equals(sI1))
                  sId = getSqlColumn((Column) et.getRightExpression(), lstTabAlias, dftAlias, appendOpt, lstAliasOfSimpleReference);
                else if (et.getRightExpression().toString().equals(sI1))
                  sId = getSqlColumn((Column) et.getLeftExpression(), lstTabAlias, dftAlias, appendOpt, lstAliasOfSimpleReference);
              }
              if (on instanceof Parenthesis) {
                Parenthesis p = (Parenthesis) on;
                if (p.getExpression() instanceof EqualsTo) {
                  EqualsTo et = (EqualsTo) p.getExpression();
                  if (et.getLeftExpression().toString().equals(s1) || et.getLeftExpression().toString().equals(s2))
                    filterPara = getSqlColumn((Column) et.getRightExpression(), lstTabAlias, dftAlias, appendOpt, lstAliasOfSimpleReference);
                  else if (et.getRightExpression().toString().equals(s1) || et.getRightExpression().toString().equals(s2))
                    filterPara = getSqlColumn((Column) et.getLeftExpression(), lstTabAlias, dftAlias, appendOpt, lstAliasOfSimpleReference);
                }
              }
            }
            if (filterPara != null || sId != null) {
              String prevTable = (i == 0 ? ((Table) plain.getFromItem()).getName() : ((Table) plain.getJoins().get(i - 1).getRightItem()).getName());
              sv = sm != null ? sm.getVirtualCollections().get(prevTable) : null;
              if (sv == null || !sv.getCollectionName().equals(fromItem.getName())) {
                sb.append(" LET ");
                if (fromItem.getAlias() != null && fromItem.getAlias().getName() != null &&
                  !fromItem.getAlias().getName().equals(fromItem.getName()))
                  alias = fromItem.getAlias().getName();
                else
                  alias = "c" + (appendOpt.collectionNo++);
                lstTabAlias.put(fromItem.getName(), alias);
                if (sId != null)
                  sb.append(alias).append("=DOCUMENT(").append(sId).append(")");
                else
                  sb.append(alias).append("=DOCUMENT('").append(getAliasCollection(fromItem.getName())).append("',").append(filterPara).append(")");
                if (!j.isOuter() && !j.isInner())
                  sb.append(" FILTER ").append(alias).append(sId != null ? "._id" : "._key");
              }
            } else if (j.isOuter()) {
              sb.append(" LET ");
              if (fromItem.getAlias() != null && fromItem.getAlias().getName() != null &&
                !fromItem.getAlias().getName().equals(fromItem.getName())) {
                alias = fromItem.getAlias().getName();
                appendOpt.sqlAlias = alias;
              } else
                alias = "c" + (appendOpt.collectionNo++);
              String oAlias = "c" + (appendOpt.collectionNo++);
              sb.append(alias).append("=(FOR ").append(oAlias).append(" IN ").append(getAliasCollection(fromItem.getName())).append(" FILTER ");
              lstTabAlias.put(fromItem.getName(), oAlias);
              for (Expression on : j.getOnExpressions()) {
                // alias in sql change in new alias
                if (appendOpt.sqlAlias != null)
                  appendOpt.aqlAlias = oAlias;
                sb.append(appendExpression(on, lstTabAlias, dftAlias, appendOpt, plain.getGroupBy() != null, sm, lstAliasOfSimpleReference));
              }
              appendOpt.sqlAlias = null;
              appendOpt.aqlAlias = null;
              sb.append(" RETURN ").append(oAlias).append(")");
              lstTabAlias.put(fromItem.getName(), alias);
            } else {
              sb.append(" FOR ");
              if (fromItem.getAlias() != null && fromItem.getAlias().getName() != null &&
                !fromItem.getAlias().getName().equals(fromItem.getName()))
                alias = fromItem.getAlias().getName();
              else
                alias = "c" + (appendOpt.collectionNo++);
              sb.append(alias).append(" IN ").append(getAliasCollection(fromItem.getName())).append(" FILTER ");
              lstTabAlias.put(fromItem.getName(), alias);
              for (Expression on : j.getOnExpressions()) {
                sb.append(appendExpression(on, lstTabAlias, dftAlias, appendOpt, plain.getGroupBy() != null, sm, lstAliasOfSimpleReference));
              }
            }
          }
        } else {
          sb.append(" FOR ");
          if (fromItem.getAlias() != null && fromItem.getAlias().getName() != null &&
            !fromItem.getAlias().getName().equals(fromItem.getName()))
            alias = fromItem.getAlias().getName();
          else
            alias = "c" + (appendOpt.collectionNo++);
          sb.append(alias).append(" IN ").append(getAliasCollection(fromItem.getName()));
          lstTabAlias.put(fromItem.getName(), alias);
        }
      }
    }
    // List of defined column alias. for group "return" output
    HashMap<String, String> lstColAlias = plain.getSelectItems().stream().
      filter(si -> si.getExpression() instanceof Column && si.getAlias() != null).
      collect(Collectors.toMap(si -> si.getExpression().toString(), si -> si.getAlias().getName(), (a, b) -> b, HashMap::new));
    // List of defined column as function with alias. for collect aggregate
    HashMap<String, String> lstColAggAlias = new HashMap<>();
    ArrayList<ColInfo> lstAggCols = new ArrayList<>();
    for (SelectItem si : plain.getSelectItems()) {
      if (si.getExpression() instanceof Function) {
        Function func = (Function) si.getExpression();
        String fName = func.getName().toUpperCase();
        if (aggregateSqlFunc.contains(fName) && (!fName.equals("LEN") || plain.getGroupBy() != null)) {
          String ag = appendExpression(si.getExpression(), lstTabAlias, dftAlias, appendOpt, plain.getGroupBy() != null, sm, lstAliasOfSimpleReference);
          lstColAggAlias.put(si.getAlias() != null ? si.getAlias().getName() : fName, ag);
          if ("COUNT".equals(fName))
            lstAggCols.add(new ColInfo(si.getAlias() != null ? si.getAlias().getName() : fName, "Integer", Types.INTEGER, Integer.class.toString()));
          else
            lstAggCols.add(new ColInfo(si.getAlias().getName(), "Double", Types.DOUBLE, Double.class.toString()));
        }
      } else if (si.getExpression() instanceof Column)
        lstAggCols.add(new ColInfo(((Column) si.getExpression()).getColumnName(), "String", Types.VARCHAR, String.class.toString()));
    }
    if (plain.getWhere() != null) {
      sb.append(" FILTER ");
      sb.append(appendExpression(plain.getWhere(), lstTabAlias, dftAlias, appendOpt, plain.getGroupBy() != null, sm, lstAliasOfSimpleReference));
    }
    // GROUP BY / HAVING => COLLECT / AGGREGATE
    StringBuilder gSb = null;
    HashMap<String, String> lstColAliasGroup = new HashMap<>();
    if (plain.getGroupBy() != null) {
      sb.append(" COLLECT ");
      boolean first = true;
      List<Expression> lstGrp = plain.getGroupBy().getGroupByExpressionList().getExpressions();
      for (int g = 0; g < lstGrp.size(); g++) {
        Expression gExp = lstGrp.get(g);
        if (gExp instanceof Column) {
          Column c = (Column) gExp;
          if (first)
            first = false;
          else
            sb.append(",");
          sb.append("g").append(g).append("=");
          sb.append(getSqlColumn(c, lstTabAlias, dftAlias, appendOpt, lstAliasOfSimpleReference));
          ColInfo ci = null;
          for (ColInfo cI : lstAggCols) {
            if (cI.getName().equals(c.getColumnName())) {
              ci = cI;
              break;
            }
          }
          if (connection != null && ci != null) {
            if (sm != null) {
              CollectionSchema colSchema = sm.getSchema(findTableName(c.getTable() != null ? c.getTable().getName() : dftTabName, lstTabAlias));
              if (colSchema != null) {
                for (SchemaNode sn : colSchema.getProperties()) {
                  if (sn.getName().equals(c.getColumnName())) {
                    if (sn.getDataType().get(0) != Types.VARCHAR) {
                      ci.setType(sn.getDataType().get(0));
                    }
                    break;
                  }
                }
              }
            }
          }
          if (gSb == null)
            gSb = new StringBuilder("{");
          else
            gSb.append(",");
          lstColAliasGroup.put(gExp.toString(), "g" + g);
          if (lstColAlias.containsKey(gExp.toString())) {
            String a = lstColAlias.get(gExp.toString());
            gSb.append(a);
            ci.setName(a);
            // add alias column name for group/collect
            if (lstColAliasGroup.containsKey(gExp.toString()))
              lstColAliasGroup.put(lstColAlias.get(gExp.toString()), "g" + g);
          } else
            gSb.append(getSqlColumn((Column) gExp, null, null, appendOpt, lstAliasOfSimpleReference));
          gSb.append(":").append("g").append(g);
        }
      }
      // Add aggregate column to return
      for (String key : lstColAggAlias.keySet()) {
        String ag = lstColAggAlias.get(key);
        if (gSb == null)
          gSb = new StringBuilder("{");
        else
          gSb.append(",");
        gSb.append(key).append(":").append(ag);
      }
      if (gSb != null)
        gSb.append("}");
      if (appendOpt.aggregate != null || plain.getHaving() != null) {
        String agFilter = null;
        if (plain.getHaving() != null)
          agFilter = appendExpression(plain.getHaving(), lstTabAlias, dftAlias, appendOpt, plain.getGroupBy() != null, sm, lstAliasOfSimpleReference);
        sb.append(" AGGREGATE ").append(appendOpt.aggregate);
        if (agFilter != null)
          sb.append(" FILTER ").append(agFilter);
        appendOpt.aggregate = null;
      }
      lstRCols.addAll(lstAggCols);
    } else if (appendOpt.aggregate != null) {
      sb.append(" COLLECT AGGREGATE ").append(appendOpt.aggregate);
      appendOpt.aggregate = null;
      // Add aggregate column to return
      for (String key : lstColAggAlias.keySet()) {
        String ag = lstColAggAlias.get(key);
        if (gSb == null)
          gSb = new StringBuilder("{");
        else
          gSb.append(",");
        gSb.append(key).append(":").append(ag);
      }
      if (gSb != null)
        gSb.append("}");
      lstRCols.addAll(lstAggCols);
    }

    if (plain.getOrderByElements() != null) {
      sb.append(" SORT ");
      boolean first = true;
      for (OrderByElement o : plain.getOrderByElements()) {
        if (first)
          first = false;
        else
          sb.append(",");
        if (lstColAliasGroup.containsKey(o.getExpression().toString()))
          sb.append(lstColAliasGroup.get(o.getExpression().toString()));
        else
          sb.append(appendExpression(o.getExpression(), lstTabAlias, dftAlias, appendOpt, plain.getGroupBy() != null, sm, lstAliasOfSimpleReference));
        if (!o.isAsc())
          sb.append(" desc");
      }
    }
    if (maxRows > 0 && appendOpt.additionalLstTabAlias == null)
      sb.append(" LIMIT ").append(maxRows);

    // get schema information from all collections
    HashMap<String, HashMap<String, ColInfo>> lstColsDesc = gSb == null ? readCollectionSchema(lstTabAlias.keySet()) : null;

    sb.append(" RETURN ");
    if (gSb != null)
      sb.append(gSb);
    else if (plain.getSelectItems().get(0).getExpression() instanceof AllColumns) {
      if (lstTabAlias.size() > 1)
        sb.append("MERGE(");
      boolean first = true;
      for (String tab : lstTabAlias.keySet()) {
        String a = lstTabAlias.get(tab);
        if (first)
          first = false;
        else
          sb.append(",");
        sb.append(a);
        if (lstColsDesc.get(tab) != null)
          lstRCols.addAll(lstColsDesc.get(tab).values());
      }
//            if (lstTabAlias.size() > 1) {
//              boolean first = true;
//              for (String a : lstTabAlias.values()) {
//                if (first)
//                  first = false;
//                else
//                  sb.append(",");
//                sb.append(a);
//              }
//            } else
//              sb.append(dftAlias);
      if (lstTabAlias.size() > 1)
        sb.append(")");
    } else {
      // when sub select (additionalLstTabAlias != null) then only one column return
      if (appendOpt.additionalLstTabAlias == null)
        sb.append("{");
      // List of "normal" columns
      ArrayList<SelectItem> lstAlg = new ArrayList<>();
      // List of columns must group in object, e.g. description:{DE:...}
      HashMap<String, ArrayList<SelectItem>> lstSep = new HashMap<>();
      for (SelectItem si : plain.getSelectItems()) {
        if (si.getExpression() instanceof Column) {
          Column col = (Column) si.getExpression();
          if (col.getTable() != null) {
            String tab = col.getTable().toString();
            if (tab.contains(".")) {
              ArrayList<SelectItem> a = lstSep.computeIfAbsent(tab, k -> new ArrayList<>());
              a.add(si);
              continue;
            }
          }
        }
        lstAlg.add(si);
      }
      boolean first = true;
      for (SelectItem si : lstAlg) {
        if (first)
          first = false;
        else if (appendOpt.additionalLstTabAlias != null)
          break;
        else
          sb.append(",");
        if (appendOpt.additionalLstTabAlias == null) {
          if (si.getAlias() != null && si.getAlias().getName() != null)
            sb.append(si.getAlias().getName());
          else if (si.getExpression() instanceof Column)
            sb.append(modifyColumnName((Column) si.getExpression()).replaceAll("\"", ""));
          sb.append(":");
        }
        sb.append(appendExpression(si.getExpression(), lstTabAlias, dftAlias, appendOpt, plain.getGroupBy() != null, sm, lstAliasOfSimpleReference));
        // Search ColInfo for ResultMetaData
        if (si.getExpression() instanceof Column) {
          Column column = (Column) si.getExpression();
          HashMap<String, ColInfo> tabCols = lstColsDesc.get(column.getTable() != null ? column.getTable().getName() : dftTabName);
          if (tabCols != null) {
            ColInfo ci = tabCols.get(modifyColumnName(column));
            if (ci == null)
              ci = new ColInfo(modifyColumnName(column), "NVARCHAR", Types.VARCHAR, String.class.getName());
            lstRCols.add(ci);
          }
        }
      }
      for (String key : lstSep.keySet()) {
        boolean start = true;
        int bc = 0;
        for (SelectItem si : lstSep.get(key)) {
          if (first)
            first = false;
          else if (appendOpt.additionalLstTabAlias != null)
            break;
          else
            sb.append(",");
          Column col = (Column) si.getExpression();
          if (start) {
            String[] tn = col.getTable().toString().split("\\.");
            for (int t = 1; t < tn.length; t++) {
              sb.append(tn[t]).append(":{");
            }
            bc = tn.length - 1;
            start = false;
          }
          if (si.getAlias() != null && si.getAlias().getName() != null)
            sb.append(si.getAlias().getName());
          else
            sb.append(modifyColumnName(col).replaceAll("\"", ""));
          sb.append(":");
          sb.append(appendExpression(si.getExpression(), lstTabAlias, dftAlias, appendOpt, plain.getGroupBy() != null, sm, lstAliasOfSimpleReference));
          // Search ColInfo for ResultMetaData
          if (si.getExpression() instanceof Column) {
            String[] tn = col.getTable().toString().split("\\.");
            HashMap<String, ColInfo> tabCols = lstColsDesc.get(tn[0]);
            if (tabCols != null) {
              StringBuilder sbCol = new StringBuilder();
              for (int cn = 1; cn < tn.length; cn++)
                sbCol.append(tn[cn]).append(".");
              sbCol.append(modifyColumnName(col));
              ColInfo ci = tabCols.get(sbCol.toString());
              if (ci == null)
                ci = new ColInfo(modifyColumnName(col), "NVARCHAR", Types.VARCHAR, String.class.getName());
              lstRCols.add(ci);
            }
          }
        }
        for (; bc > 0; bc--)
          sb.append("}");
      }
      if (appendOpt.additionalLstTabAlias == null)
        sb.append("}");
    }
    return sb.toString();
  }

  private String findTableName(String tabName, Map<String, String> lstTabAlias) {
    if (lstTabAlias.containsValue(tabName)) {
      for (String tName : lstTabAlias.keySet()) {
        if (lstTabAlias.get(tName).equals(tabName)) {
          tabName = tName;
          break;
        }
      }
    }
    return tabName;
  }

  private String appendExpression(Expression exp, HashMap<String, String> lstTabAlias, String dftAlias, AppendOption appendOpt, boolean withGroup, StructureManager sm, List<String> lstRefAlias) {
    if (exp instanceof Column) {
      return getSqlColumn((Column) exp, lstTabAlias, dftAlias, appendOpt, lstRefAlias);
    } else if (exp instanceof AndExpression) {
      AndExpression and = (AndExpression) exp;
      return "(" + appendExpression(and.getLeftExpression(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias) + " && " +
        appendExpression(and.getRightExpression(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias) + ")";
    } else if (exp instanceof OrExpression) {
      OrExpression or = (OrExpression) exp;
      return "(" + appendExpression(or.getLeftExpression(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias) + " || " +
        appendExpression(or.getRightExpression(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias) + ")";
    } else if (exp instanceof ComparisonOperator) {
      ComparisonOperator comp = (ComparisonOperator) exp;
      String op = comp.getStringExpression();
      if ("=".equals(op))
        op = "==";
      else if ("<>".equals(op))
        op = "!=";
      if (sm != null && comp.getLeftExpression() instanceof Column && (comp.getRightExpression() instanceof DoubleValue || comp.getRightExpression() instanceof LongValue)
        && lstBoolValue.contains(comp.getRightExpression().toString())) {
        CollectionSchema cs = sm.getSchema(findTableName(((Column) comp.getLeftExpression()).getTable().getName(), lstTabAlias));
        if (cs != null) {
          String colName = ((Column) comp.getLeftExpression()).getColumnName();
          for (SchemaNode sn : cs.getProperties()) {
            if (sn.getName().equals(colName)) {
              if (sn.getDataType().get(0) == Types.BOOLEAN) {
                return appendExpression(comp.getLeftExpression(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias) + op +
                  (comp.getRightExpression().toString().equals("1") ? "true" : "false");
              }
              break;
            }
          }
        }
      }
      return appendExpression(comp.getLeftExpression(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias) + op +
        appendExpression(comp.getRightExpression(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias);
    } else if (exp instanceof LikeExpression) {
      LikeExpression like = (LikeExpression) exp;
      return (like.isNot() ? "!" : "") + "LIKE(" + appendExpression(like.getLeftExpression(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias) + "," +
        appendExpression(like.getRightExpression(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias) + ")";
    } else if (exp instanceof Between) {
      Between between = (Between) exp;
      String col = appendExpression(between.getLeftExpression(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias);
      String from = appendExpression(between.getBetweenExpressionStart(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias);
      String to = appendExpression(between.getBetweenExpressionEnd(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias);
      // If the comparison is designed exclusively for one day, then set the time to the end of the day.
      if (from.equals(to) && between.getBetweenExpressionEnd() instanceof Function &&
        ((Function) between.getBetweenExpressionEnd()).getName().equalsIgnoreCase("TIMESTAMP") &&
        !((Function) between.getBetweenExpressionEnd()).getParameters().isEmpty() &&
        ((Function) between.getBetweenExpressionEnd()).getParameters().get(0) instanceof StringValue) {
        Function func = (Function) between.getBetweenExpressionEnd();
        String dValue = ((StringValue) func.getParameters().get(0)).getValue();
        String tValue = func.getParameters().size() > 1 ? ((StringValue) func.getParameters().get(1)).getValue() : null;
        if (dValue.contains(" ")) {
          String[] dt = dValue.split(" ");
          dValue = dt[0];
          tValue = dt[1];
        }
        if (("00:00:00").equals(tValue)) {
          tValue = "23:59:59";
          to = convertSQLDateTime(dValue, tValue);
          to = to.substring(0, to.indexOf(".")) + ".999Z'";
        }
      }
      return (between.isNot() ? "!(" : "(") + col + ">=" + from + " && " + col + "<=" + to + ")";
    } else if (exp instanceof InExpression) {
      InExpression in = (InExpression) exp;
      String inValue;
      if (in.getRightExpression() instanceof ParenthesedExpressionList) {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (Object expItem : ((ParenthesedExpressionList) in.getRightExpression())) {
          if (expItem instanceof Expression) {
            if (first)
              first = false;
            else
              sb.append(",");
            sb.append(appendExpression((Expression) expItem, lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias));
          }
        }
        sb.append("]");
        inValue = sb.toString();
      } else if (in.getRightExpression() instanceof ParenthesedSelect) {
        String subSql = appendExpression(in.getRightExpression(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias);
        inValue = "(" + subSql.substring(0, subSql.length() - 3) + ")";
      } else
        inValue = "[]";
      return appendExpression(in.getLeftExpression(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias) + (in.isNot() ? " NOT " : "") + " IN " + inValue;
    } else if (exp instanceof IsNullExpression) {
      IsNullExpression isNull = (IsNullExpression) exp;
      return (isNull.isNot() ? "" : "!") + appendExpression(isNull.getLeftExpression(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias);
    } else if (exp instanceof StringValue) {
      StringValue value = (StringValue) exp;
      return "'" + value.getValue() + "'";
    } else if (exp instanceof DoubleValue || exp instanceof LongValue) {
      return exp.toString();
    } else if (exp instanceof DateValue) {
      DateValue date = (DateValue) exp;
      Date dat = new Date(date.getValue().getTime());
      return dat.toInstant().toString();
    } else if (exp instanceof TimestampValue) {
      TimestampValue date = (TimestampValue) exp;
      Date dat = new Date(date.getValue().getTime());
      return dat.toInstant().toString();
    } else if (exp instanceof TimeValue) {
      TimeValue date = (TimeValue) exp;
      Time dat = new Time(date.getValue().getTime());
      return dat.toInstant().toString();
    } else if (exp instanceof TimeKeyExpression) {
      TimeKeyExpression date = (TimeKeyExpression) exp;
      if ("CURRENT_TIMESTAMP".equalsIgnoreCase(date.getStringValue()))
        return "DATE_ISO8601(DATE_NOW())";
    } else if (exp instanceof JdbcParameter) {
      return "@p" + ((JdbcParameter) exp).getIndex();
    } else if (exp instanceof Function) {
      Function func = (Function) exp;
      String funcName = func.getName().toUpperCase();
      // If the Timestamp function is called with a parameter, adjust this accordingly for the AQL date.
      if ("TIMESTAMP".equals(funcName) && !func.getParameters().isEmpty() && func.getParameters().get(0) instanceof StringValue) {
        String dValue = ((StringValue) func.getParameters().get(0)).getValue();
        String tValue = func.getParameters().size() > 1 ? ((StringValue) func.getParameters().get(1)).getValue() : null;
        if (tValue == null) {
          String[] dt = dValue.split(" ");
          dValue = dt[0];
          tValue = dt[1];
        }
        return convertSQLDateTime(dValue, tValue);
      }
      if (aggregateSqlFunc.contains(funcName) && (!"LEN".equals(funcName) || withGroup)) {
        String ag = "ag" + (++appendOpt.aggregateNo);
        if (appendOpt.aggregate == null)
          appendOpt.aggregate = new StringBuilder();
        else
          appendOpt.aggregate.append(",");
        String expFunc = "";
        if (func.getParameters() != null && !func.getParameters().isEmpty() && !(func.getParameters().get(0) instanceof AllColumns))
          expFunc = appendExpression(func.getParameters().get(0), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias);
        appendOpt.aggregate.append(ag).append("=").append(func.getName()).append("(").append(expFunc).append(")");
        return ag;
      }
      AQLFunction aqlFunc = mapSQLFuncToAQLFunc.get(funcName);
      StringBuilder f = new StringBuilder(aqlFunc != null ? aqlFunc.name : funcName);
      f.append("(");
      List<String> lstParam = new ArrayList<>();
      for (Expression para : func.getParameters()) {
        lstParam.add(appendExpression(para, lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias));
      }
      if (aqlFunc != null && aqlFunc.parameterOrder != null) {
        List<String> lstParamOrdered = new ArrayList<>();
        for (int i = 0; i < aqlFunc.parameterOrder.length; ++i) {
          if (aqlFunc.parameterOrder[i] > 0)
            lstParamOrdered.add(lstParam.get(aqlFunc.parameterOrder[i] - 1));
        }
        lstParam = lstParamOrdered;
      }
      f.append(String.join(",", lstParam));
      f.append(")");
      return f.toString();
    } else if (exp instanceof ParenthesedSelect) {
      ParenthesedSelect sub = (ParenthesedSelect) exp;
      PlainSelect plain = sub.getPlainSelect();
      appendOpt.additionalLstTabAlias = lstTabAlias;
      HashMap<String, String> subLstTabAlias = new HashMap<>();
      ArrayList<ColInfo> lstRCols = new ArrayList<>();
      String sql = "(" + getPlainSelect(plain, subLstTabAlias, lstRCols, appendOpt) + ")[0]";
      appendOpt.additionalLstTabAlias = null;
      return sql;
    } else if (exp instanceof UserVariable) {
      return "@" + ((UserVariable) exp).getName();
    } else if (exp instanceof Parenthesis) {
      Parenthesis par = (Parenthesis) exp;
      return appendExpression(par.getExpression(), lstTabAlias, dftAlias, appendOpt, withGroup, sm, lstRefAlias);
    } else
      System.err.println("Not implement SQL Expression : " + exp.getClass().toString());
    return "";
  }

  private String convertSQLDateTime(String dValue, String tValue) {
    String[] dV = dValue.split("-");
    String[] tV = tValue.split(":");
    Calendar cal = Calendar.getInstance();
    cal.set(Integer.parseInt(dV[0]), Integer.parseInt(dV[1]) - 1, Integer.parseInt(dV[2]), Integer.parseInt(tV[0]), Integer.parseInt(tV[1]), Integer.parseInt(tV[2]));
    Calendar utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    utcCal.setTimeInMillis(cal.getTimeInMillis());
    String d = utcCal.getTime().toInstant().toString();
    d = d.contains(".") ? d.substring(0, d.indexOf(".")) + ".000Z" : d.substring(0, d.length() - 1) + ".000Z";
    return "'" + d + "'";
  }

  private String getSqlColumn(Column col, HashMap<String, String> lstTabAlias, String dftAlias, AppendOption appendOpt, List<String> lstRefAlias) {
    String tab;
    if (col.getTable() != null) {
      tab = lstTabAlias.get(col.getTable().toString());
      if (tab == null) {
        tab = col.getTable().toString();
        if (tab.contains(".")) {
          String t1 = tab.substring(0, tab.indexOf("."));
          String t2 = lstTabAlias.get(t1);
          if (t2 != null)
            tab = t2 + tab.substring(tab.indexOf("."));
          else if (appendOpt.sqlAlias != null && appendOpt.sqlAlias.equals(t1))
            tab = appendOpt.aqlAlias + tab.substring(tab.indexOf("."));
          else if (appendOpt.additionalLstTabAlias != null) {
            String a2 = appendOpt.additionalLstTabAlias.get(t1);
            if (a2 != null)
              tab = a2 + tab.substring(tab.indexOf("."));
          }
        } else if (appendOpt.sqlAlias != null && appendOpt.sqlAlias.equals(tab))
          tab = appendOpt.aqlAlias;
        else if (appendOpt.additionalLstTabAlias != null) {
          String tb = appendOpt.additionalLstTabAlias.get(tab);
          if (tb != null)
            tab = tb;
        }
      }
    } else
      tab = dftAlias;
    if (lstRefAlias != null && lstRefAlias.contains(tab) && "entryValue".equals(col.getColumnName()))
      return tab;
    return (tab != null ? tab + "." : "") + modifyColumnName(col).replaceAll("\"", "");
  }

  private String modifyColumnName(Column col) {
    if (separatorStructColumn != null)
      return col.getColumnName().replaceAll(separatorStructColumn, ".");
    return col.getColumnName();
  }

  private HashMap<String, HashMap<String, ColInfo>> readCollectionSchema(Collection<String> collections) {
    HashMap<String, HashMap<String, ColInfo>> lstColsDesc = new HashMap<>();
    if (connection != null) {
      StructureManager sm = connection.getStructureManager();
      collections.forEach(col -> {
        SchemaVirtual sv = sm.getVirtualCollections().get(col);
        if (sv != null) {
          CollectionSchema schema = sm.getSchema(sv.getCollectionName());
          SchemaNode node = schema.getProperties().stream()
            .filter(n -> n.getName().equals(sv.getColumnName()))
            .findFirst()
            .orElse(null);
          if (node != null) {
            lstColsDesc.put(col, sm.getColInfo(sv.getCollectionName(), node.getReferences().get(0)));
          }
        } else
          lstColsDesc.put(col, sm.getColInfo(col));
      });
    }
    return lstColsDesc;
  }

  protected String getAliasCollection(String alias) {
    if (connection != null)
      return connection.getAliasCollection(alias);
    return alias;
  }

  protected String getCollectionAlias(String collection) {
    if (connection != null)
      return connection.getCollectionAlias(collection);
    return collection;
  }

}
