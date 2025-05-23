package de.hcbraun.arangodb.jdbc;

import com.arangodb.entity.CollectionEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

public class ArangoDBCollectionResultSet extends ArangoDBResultSet {

  private final Logger logger = LoggerFactory.getLogger(ArangoDBCollectionResultSet.class);

  Collection<String> lstCol;
  String curCol = null;
  Iterator<String> it = null;
  private String schema = "adbdbo";
  private ArangoDBConnection connection;

  protected ArangoDBCollectionResultSet(Collection<String> lstCol, String schema, ArangoDBConnection connection) {
    super(null, null, new ArangoDBResultSetMetaData("// cols: TABLE_CAT:s,TABLE_SCHEM:s,TABLE_NAME:s," +
            "TABLE_TYPE:s,REMARKS:s,TYPE_CAT:s,TYPE_SCHEM:s,TYPE_NAME:s,SELF_REFERENCING_COL_NAME:s,REF_GENERATION:s"));
    this.lstCol = lstCol;
    this.schema = schema;
    this.connection = connection;
    logger.debug("Collection-Count: " + lstCol.size() + " / " + schema);
  }

  @Override
  public boolean next() throws SQLException {
    if(it == null)
      it = lstCol.iterator();
    if(it.hasNext())
      curCol = it.next();
    else
      curCol = null;
    if (curCol != null)
      logger.debug("next-Row: " + curCol);
    return curCol != null;
  }

  @Override
  public boolean first() throws SQLException {
    logger.debug("first");
    it = lstCol.iterator();
    return it.hasNext();
  }

  @Override
  protected Object getFieldValue(String selectExpression) {
    switch (selectExpression) {
      case "TABLE_NAME":
        return connection.getCollectionAlias(curCol);
      case "TABLE_TYPE":
        return "TABLE";
      case "REMARKS":
        return "";
      case "TABLE_SCHEM":
        return schema;
      case "TABLE_CAT":
      case "TYPE_CAT":
      case "TYPE_SCHEM":
      case "TYPE_NAME":
      case "SELF_REFERENCING_COL_NAME":
      case "REF_GENERATION":
      default:
        return null;
    }
  }
}
