package de.hcbraun.arangodb.jdbc;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;

import java.sql.Types;
import java.util.*;

public class StructureManager {

  ArangoDBConnection connection;
  int refreshTime = 180;

  Map<String, CollectionSchema> schemaMap = new HashMap<>();

  StructureManager(ArangoDBConnection connection) {
    this.connection = connection;
  }

  public int getRefreshTime() {
    return refreshTime;
  }

  public void setRefreshTime(int refreshTime) {
    this.refreshTime = refreshTime;
  }

  public CollectionSchema getSchema(String collection) {
    CollectionSchema schema = schemaMap.get(collection);
    if (schema == null || (schema.getNextRefresh() > 0 && schema.getNextRefresh() < System.currentTimeMillis())) {
      try {
        String col = connection.getAliasCollection(collection);
        ArangoCursor<BaseDocument> cursor = connection.getDatabase().query("RETURN SCHEMA_GET('" + col + "')", BaseDocument.class);
        if (cursor != null) {
          schema = analyseSchema(collection, col, cursor.next());
          if (!col.equals(collection))
            schemaMap.put(col, schema);
          schemaMap.put(collection, schema);
        }
      } catch (ArangoDBException e) {
        e.printStackTrace();
      }
    }
    return schema;
  }

  public CollectionSchema getSchema(String collection, BaseDocument docSchema) {
    CollectionSchema schema = schemaMap.get(collection);
    if (schema == null || (schema.getNextRefresh() > 0 && schema.getNextRefresh() < System.currentTimeMillis())) {
      String col = connection.getAliasCollection(collection);
      schema = analyseSchema(collection, col, docSchema);
      if (!col.equals(collection))
        schemaMap.put(col, schema);
      schemaMap.put(collection, schema);
    }
    return schema;
  }

  private CollectionSchema analyseSchema(String collection, String col, BaseDocument doc) {
    Map<String, Object> rule = (Map) doc.getAttribute("rule");
    Map<String, Object> props = (Map) rule.get("properties");
    Map<String, Object> defs = (Map) rule.get("$defs");

    CollectionSchema schema = new CollectionSchema(col);
    if (!col.equals(collection))
      schema.setAliasName(collection);

    // all additional definition used by $ref
    if (defs != null) {
      HashMap<String, SchemaReference> refs = new HashMap<>();
      defs.forEach((k, v) -> {
        Map<String, Object> defProps = (Map) ((Map<String, Object>) v).get("properties");
        if (defProps != null)
          refs.put(k, new SchemaReference(k, createListOfNodes(defProps)));
        else {
          Map<String, Object> defValue = (Map) ((Map<String, Object>) v).get("value");
          if (defValue != null) {
            String mDt = (String) defValue.get("type");
            String mDf = (String) defValue.get("format");
            Number mMultipleOf = (Number) defValue.get("multipleOf");
            SchemaDatatype dt = new SchemaDatatype(k, findDataType(mDt, mDf, mMultipleOf));
            if (schema.getDatatypes() == null)
              schema.setDatatypes(new HashMap<>());
            schema.getDatatypes().put(k, dt);
          }
        }
      });
    }
    if (props != null) {
      schema.setProperties(createListOfNodes(props));
    }
    if (refreshTime > 0)
      schema.setNextRefresh(System.currentTimeMillis() + refreshTime * 60 * 1000L);
    return schema;
  }

  private List<SchemaNode> createListOfNodes(Map<String, Object> props) {
    List<SchemaNode> nodes = new ArrayList<>();
    props.forEach((k, v) -> {
      SchemaNode node = new SchemaNode(k);
      Map<String, Object> m = (Map) v;
      String dt = (String) m.get("type");
      String ref = (String) m.get("$ref");
      String df = (String) m.get("format");
      List lstOneOf = (List) m.get("oneOf");
      List enumType = (List) m.get("enum");
      Number multipleOf = (Number) m.get("multipleOf");
      Object uProp = m.get("properties");

      if (ref != null) {
        ArrayList<Integer> dtList = new ArrayList<>();
        dtList.add(Types.STRUCT);
        node.setDataType(dtList);
        ArrayList<String> refList = new ArrayList<>();
        refList.add(ref.substring(ref.lastIndexOf('/') + 1));
        node.setReferences(refList);
      } else if (lstOneOf != null && lstOneOf.size() > 0) {
        ArrayList<Integer> dtList = new ArrayList<>();
        ArrayList<String> refList = new ArrayList<>();
        lstOneOf.forEach(o -> {
          Map<String, Object> moo = (Map) o;
          String mDt = (String) moo.get("type");
          String mRef = (String) moo.get("$ref");
          String mDf = (String) moo.get("format");
          Number mMultipleOf = (Number) moo.get("multipleOf");
          if ("null".equalsIgnoreCase(mDt)) {
            node.setNullable(true);
          } else if (mRef != null) {
            dtList.add(Types.STRUCT);
            refList.add(mRef.substring(mRef.lastIndexOf('/') + 1));
          } else {
            dtList.add(findDataType(mDt, mDf, mMultipleOf));
          }
        });
        if (!dtList.isEmpty())
          node.setDataType(dtList);
        if (!refList.isEmpty())
          node.setReferences(refList);
      } else if (enumType != null && !enumType.isEmpty()) {
        int dataType = Types.NULL;
        for (Object o : enumType) {
          if ("null".equalsIgnoreCase(o.toString())) {
            node.setNullable(true);
          } else {
            if ((dataType == Types.NULL || dataType == Types.BOOLEAN) && o instanceof Number) {
              dataType = Types.DOUBLE;
            } else if (o instanceof String) {
              dataType = Types.VARCHAR;
            } else if (dataType == Types.NULL && o instanceof Boolean) {
              dataType = Types.BOOLEAN;
            }
          }
        }
        ArrayList<Integer> dtList = new ArrayList<>();
        dtList.add(dataType);
        node.setDataType(dtList);
        node.setEnumValues(enumType);
      } else {
        ArrayList<Integer> dtList = new ArrayList<>();
        dtList.add(findDataType(dt, df, multipleOf));
        node.setDataType(dtList);
      }
      if (uProp instanceof Map) {
        node.setProperties(createListOfNodes((Map<String, Object>) uProp));
      }
      nodes.add(node);
    });
    return nodes;
  }

  private int findDataType(String dt, String df, Number multipleOf) {
    if ("string".equalsIgnoreCase(dt)) {
      if ("YYYY-MM-DDTHH:MM:SSZ".equalsIgnoreCase(df) || "yyyy-MM-ddTHH:mm:ss.SSSZ".equalsIgnoreCase(df))
        return Types.TIMESTAMP;
      else if ("YYYY-MM-DD".equalsIgnoreCase(df))
        return Types.DATE;
      else if ("HH:MM".equalsIgnoreCase(df) || "HH:MM:SS".equalsIgnoreCase(df) || "HH:MM:SS.SSS".equalsIgnoreCase(df))
        return Types.TIME;
    } else if ("integer".equalsIgnoreCase(dt))
      return Types.INTEGER;
    else if ("number".equalsIgnoreCase(dt)) {
      if (multipleOf != null) {
        if (multipleOf instanceof Integer || multipleOf.doubleValue() % 1 == 0)
          return Types.INTEGER;
      }
      return Types.DOUBLE;
    } else if ("boolean".equalsIgnoreCase(dt))
      return Types.BOOLEAN;
    else if ("array".equalsIgnoreCase(dt))
      return Types.ARRAY;
    return Types.VARCHAR;
  }

}
