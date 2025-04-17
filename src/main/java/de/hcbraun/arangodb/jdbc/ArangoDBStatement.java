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

  protected QueryInfo getAQL(String sql, Map<String, Object> parameters) {
    StringBuilder comments = null, newSql = null;
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
        sql = sql.replaceAll("(( |,)" + s + "\\.)" , " ");
      } catch (SQLException e) {}
    }

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
              sb.append(appendExpression(p, null, "c1", appendOpt));
          } else if (lstSelect != null) {
            SelectItem si = lstSelect.get(i);
            sb.append(appendExpression(si.getExpression(), null, "c1", appendOpt));
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
          sb.append(appendExpression(update.getWhere(), null, "c1", appendOpt));
        }
        sb.append(" UPDATE c1._key WITH {");
        for (UpdateSet us : update.getUpdateSets()) {
          List<Column> lstCol = us.getColumns();
          ExpressionList<?> lstPara = us.getValues();
          for (int i = 0; i < lstCol.size(); i++) {
            if (i > 0)
              sb.append(",");
            sb.append(getSqlColumn(lstCol.get(i), null, null, appendOpt));
            sb.append(":");
            sb.append(appendExpression(lstPara.get(i), null, "c1", appendOpt));
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
          sb.append(appendExpression(delete.getWhere(), null, "c1", appendOpt));
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

    if (comments != null)
      qi.rsmd = new ArangoDBResultSetMetaData(comments.toString());
    else if (qi.aql != null && qi.aql.startsWith("RETURN LENGTH("))
      ;
    else if (qi.aql != null) {
      String q = qi.aql;
      if ((aqlQuery || sqlSelect) && !q.toLowerCase().contains("insert ") && !q.toLowerCase().contains("update ") &&
        !q.toLowerCase().contains("remove ")) {
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
    sb.append(alias).append(" IN ").append(fromItem.getName());
    dftTabName = fromItem.getName();
    lstTabAlias.put(fromItem.getName(), alias);
    if (plain.getJoins() != null && plain.getJoins().size() > 0) {
      for (Join j : plain.getJoins()) {
        fromItem = (Table) j.getRightItem();
        if (j.getOnExpressions() != null && j.getOnExpressions().size() > 0) {
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
                  filterPara = getSqlColumn((Column) et.getRightExpression(), lstTabAlias, dftAlias, appendOpt);
                else if (et.getRightExpression().toString().equals(s1) || et.getRightExpression().toString().equals(s2))
                  filterPara = getSqlColumn((Column) et.getLeftExpression(), lstTabAlias, dftAlias, appendOpt);
                else if (et.getLeftExpression().toString().equals(sI1))
                  sId = getSqlColumn((Column) et.getRightExpression(), lstTabAlias, dftAlias, appendOpt);
                else if (et.getRightExpression().toString().equals(sI1))
                  sId = getSqlColumn((Column) et.getLeftExpression(), lstTabAlias, dftAlias, appendOpt);
              }
            }
            if (filterPara != null || sId != null) {
              sb.append(" LET ");
              if (fromItem.getAlias() != null && fromItem.getAlias().getName() != null)
                alias = fromItem.getAlias().getName();
              else
                alias = "c" + (appendOpt.collectionNo++);
              lstTabAlias.put(fromItem.getName(), alias);
              if (sId != null)
                sb.append(alias).append("=DOCUMENT(").append(sId).append(")");
              else
                sb.append(alias).append("=DOCUMENT('").append(fromItem.getName()).append("',").append(filterPara).append(")");
              if (!j.isOuter())
                sb.append(" FILTER ").append(alias).append(sId != null ? "._id" : "._key");
            } else if (j.isOuter()) {
              sb.append(" LET ");
              if (fromItem.getAlias() != null && fromItem.getAlias().getName() != null) {
                alias = fromItem.getAlias().getName();
                appendOpt.sqlAlias = alias;
              } else
                alias = "c" + (appendOpt.collectionNo++);
              String oAlias = "c" + (appendOpt.collectionNo++);
              sb.append(alias).append("=(FOR ").append(oAlias).append(" IN ").append(fromItem.getName()).append(" FILTER ");
              lstTabAlias.put(fromItem.getName(), oAlias);
              for (Expression on : j.getOnExpressions()) {
                // alias in sql change in new alias
                if (appendOpt.sqlAlias != null)
                  appendOpt.aqlAlias = oAlias;
                sb.append(appendExpression(on, lstTabAlias, dftAlias, appendOpt));
              }
              appendOpt.sqlAlias = null;
              appendOpt.aqlAlias = null;
              sb.append(" RETURN ").append(oAlias).append(")");
              lstTabAlias.put(fromItem.getName(), alias);
            } else {
              sb.append(" FOR ");
              if (fromItem.getAlias() != null && fromItem.getAlias().getName() != null)
                alias = fromItem.getAlias().getName();
              else
                alias = "c" + (appendOpt.collectionNo++);
              sb.append(alias).append(" IN ").append(fromItem.getName()).append(" FILTER ");
              lstTabAlias.put(fromItem.getName(), alias);
              for (Expression on : j.getOnExpressions()) {
                sb.append(appendExpression(on, lstTabAlias, dftAlias, appendOpt));
              }
            }
          }
        } else {
          sb.append(" FOR ");
          if (fromItem.getAlias() != null && fromItem.getAlias().getName() != null)
            alias = fromItem.getAlias().getName();
          else
            alias = "c" + (appendOpt.collectionNo++);
          sb.append(alias).append(" IN ").append(fromItem.getName());
          lstTabAlias.put(fromItem.getName(), alias);
        }
      }
    }
    if (plain.getWhere() != null) {
      sb.append(" FILTER ");
      sb.append(appendExpression(plain.getWhere(), lstTabAlias, dftAlias, appendOpt));
    }
    // GROUP BY / HAVING => COLLECT / AGGREGATE
    StringBuilder gSb = null;
    if (plain.getGroupBy() != null) {
      sb.append(" COLLECT ");
      List<Expression> lstGrp = plain.getGroupBy().getGroupByExpressionList().getExpressions();
      for (int g = 0; g < lstGrp.size(); g++) {
        Expression gExp = lstGrp.get(0);
        if (gExp instanceof Column) {
          sb.append("g").append(g).append("=");
          sb.append(getSqlColumn((Column) gExp, lstTabAlias, dftAlias, appendOpt));
          if (gSb == null)
            gSb = new StringBuilder("{");
          else
            gSb.append(",");
          gSb.append(getSqlColumn((Column) gExp, null, null, appendOpt));
          gSb.append(":").append("g").append(g);
        }
      }
      if (gSb != null)
        gSb.append("}");
      if (plain.getHaving() != null) {
        String agFilter = appendExpression(plain.getHaving(), lstTabAlias, dftAlias, appendOpt);
        if (appendOpt.aggregate != null && agFilter != null)
          sb.append(" AGGREGATE ").append(appendOpt.aggregate).append(" FILTER ").append(agFilter);
        appendOpt.aggregate = null;
      }
    }

    if (plain.getOrderByElements() != null) {
      sb.append(" SORT ");
      boolean first = true;
      for (OrderByElement o : plain.getOrderByElements()) {
        if (first)
          first = false;
        else
          sb.append(",");
        sb.append(appendExpression(o.getExpression(), lstTabAlias, dftAlias, appendOpt));
        if (!o.isAsc())
          sb.append(" desc");
      }
    }
    if (maxRows > 0 && appendOpt.additionalLstTabAlias == null)
      sb.append(" LIMIT ").append(maxRows);

    // get schema information from all collections
    HashMap<String, HashMap<String, ColInfo>> lstColsDesc = readCollectionSchema(lstTabAlias.keySet());

    sb.append(" RETURN ");
    if (gSb != null)
      sb.append(gSb);
    else if (plain.getSelectItems().get(0).getExpression() instanceof AllColumns) {
      if (lstTabAlias.size() > 1)
        sb.append("MERGE(");
      lstRCols = new ArrayList<>();
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
        sb.append(appendExpression(si.getExpression(), lstTabAlias, dftAlias, appendOpt));
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
          sb.append(appendExpression(si.getExpression(), lstTabAlias, dftAlias, appendOpt));
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

  private String appendExpression(Expression exp, HashMap<String, String> lstTabAlias, String dftAlias, AppendOption appendOpt) {
    if (exp instanceof Column) {
      return getSqlColumn((Column) exp, lstTabAlias, dftAlias, appendOpt);
    } else if (exp instanceof AndExpression) {
      AndExpression and = (AndExpression) exp;
      return appendExpression(and.getLeftExpression(), lstTabAlias, dftAlias, appendOpt) + " && " + appendExpression(and.getRightExpression(), lstTabAlias, dftAlias, appendOpt);
    } else if (exp instanceof OrExpression) {
      OrExpression or = (OrExpression) exp;
      return appendExpression(or.getLeftExpression(), lstTabAlias, dftAlias, appendOpt) + " || " + appendExpression(or.getRightExpression(), lstTabAlias, dftAlias, appendOpt);
    } else if (exp instanceof ComparisonOperator) {
      ComparisonOperator comp = (ComparisonOperator) exp;
      String op = comp.getStringExpression();
      if ("=".equals(op))
        op = "==";
      else if ("<>".equals(op))
        op = "!=";
      return appendExpression(comp.getLeftExpression(), lstTabAlias, dftAlias, appendOpt) + op + appendExpression(comp.getRightExpression(), lstTabAlias, dftAlias, appendOpt);
    } else if (exp instanceof LikeExpression) {
      LikeExpression like = (LikeExpression) exp;
      return (like.isNot() ? "!" : "") + "LIKE(" + appendExpression(like.getLeftExpression(), lstTabAlias, dftAlias, appendOpt) + "," + appendExpression(like.getRightExpression(), lstTabAlias, dftAlias, appendOpt) + ")";
    } else if (exp instanceof Between) {
      Between between = (Between) exp;
      String col = appendExpression(between.getLeftExpression(), lstTabAlias, dftAlias, appendOpt);
      return (between.isNot() ? "!(" : "(") + col + ">=" + appendExpression(between.getBetweenExpressionStart(), lstTabAlias, dftAlias, appendOpt) + " && " +
        col + "<=" + appendExpression(between.getBetweenExpressionEnd(), lstTabAlias, dftAlias, appendOpt) + ")";
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
            sb.append(appendExpression((Expression) expItem, lstTabAlias, dftAlias, appendOpt));
          }
        }
        sb.append("]");
        inValue = sb.toString();
      } else if (in.getRightExpression() instanceof ParenthesedSelect) {
        String subSql = appendExpression(in.getRightExpression(), lstTabAlias, dftAlias, appendOpt);
        inValue = "(" + subSql.substring(0, subSql.length() - 3) + ")";
      } else
        inValue = "[]";
      return appendExpression(in.getLeftExpression(), lstTabAlias, dftAlias, appendOpt) + (in.isNot() ? " NOT " : "") + " IN " + inValue;
    } else if (exp instanceof IsNullExpression) {
      IsNullExpression isNull = (IsNullExpression) exp;
      return (isNull.isNot() ? "" : "!") + appendExpression(isNull.getLeftExpression(), lstTabAlias, dftAlias, appendOpt);
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
      String ag = "ag" + (++appendOpt.aggregateNo);
      if (appendOpt.aggregate == null)
        appendOpt.aggregate = new StringBuilder();
      else
        appendOpt.aggregate.append(",");
      appendOpt.aggregate.append(ag).append("=").append(func.getName()).append("(").append(appendExpression(func.getParameters().getExpressions().get(0), lstTabAlias, dftAlias, appendOpt)).append(")");
      return ag;
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
    } else
      System.err.println("Not implement SQL Expression : " + exp.getClass().toString());
    return "";
  }

  private String getSqlColumn(Column col, HashMap<String, String> lstTabAlias, String dftAlias, AppendOption appendOpt) {
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
    return (tab != null ? tab + "." : "") + modifyColumnName(col).replaceAll("\"", "");
  }

  private String modifyColumnName(Column col) {
    if (separatorStructColumn != null)
      return col.getColumnName().replaceAll(separatorStructColumn, ".");
    return col.getColumnName();
  }

  private HashMap<String, HashMap<String, ColInfo>> readCollectionSchema(Collection<String> collections) {
    HashMap<String, HashMap<String, ColInfo>> lstColsDesc = new HashMap<>();
    if (!collections.isEmpty() && database != null) {
      HashMap<String, Object> bVars = new HashMap<>();
      bVars.put("cols", collections);
      ArangoCursor<BaseDocument> cursor = database.query("FOR c IN @cols RETURN {name: c, schema: SCHEMA_GET(c)}", BaseDocument.class, bVars);
      if (cursor != null) {
        while (cursor.hasNext()) {
          BaseDocument doc = cursor.next();
          Map<String, Object> schema = (Map) doc.getAttribute("schema");
          if (schema != null) {
            Map<String, Object> rule = (Map) schema.get("rule");
            Map<String, Object> props = (Map) rule.get("properties");
            HashMap<String, ColInfo> lstCols = new HashMap<>();
            addColumns(props, lstCols, "", (String) doc.getAttribute("name"));
            lstColsDesc.put((String) doc.getAttribute("name"), lstCols);
          }
        }
      }
    }
    return lstColsDesc;
  }

  private void addColumns(Map<String, Object> props, HashMap<String, ColInfo> cols, String prefix, String tabName) {
    for (String prop : props.keySet()) {
      Map<String, Object> m = (Map<String, Object>) props.get(prop);
      String dt = (String) m.get("type");
      String df = (String) m.get("format");
      Object uProp = m.get("properties");

      if ("object".equalsIgnoreCase(dt) && uProp != null) {
        addColumns((Map<String, Object>) uProp, cols, prefix + prop + ".", tabName);
      } else {
        int dataType = Types.VARCHAR;
        String typeName = "NVARCHAR";
        String className = String.class.getName();
        if ("string".equalsIgnoreCase(dt)) {
          if ("YYYY-MM-DDTHH:MM:SSZ".equalsIgnoreCase(df) || "'yyyy-MM-ddTHH:mm:ss.SSSZ'".equalsIgnoreCase(df)) {
            dataType = Types.TIMESTAMP;
            typeName = "NVARCHAR";
            className = String.class.getName();
          } else if ("YYYY-MM-DD".equalsIgnoreCase(df))
            dataType = Types.DATE;
          else if ("HH:MM".equalsIgnoreCase(df) || "HH:MM:SS".equalsIgnoreCase(df) || "HH:MM:SS.SSS".equalsIgnoreCase(df))
            dataType = Types.TIME;
        } else if ("integer".equalsIgnoreCase(dt))
          dataType = Types.INTEGER;
        else if ("number".equalsIgnoreCase(dt))
          dataType = Types.DOUBLE;
        else if ("boolean".equalsIgnoreCase(dt))
          dataType = Types.BOOLEAN;
        else if ("array".equalsIgnoreCase(dt))
          dataType = Types.ARRAY;

        ColInfo colI = new ColInfo(prefix + prop, typeName, dataType, className);
        colI.tabName = tabName;
        cols.put(prefix + prop, colI);
      }
    }
  }

}
