package de.hcbraun.arangodb.jdbc;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.*;

public class StructureManager {

  private final Logger logger = LoggerFactory.getLogger(StructureManager.class);

  ArangoDBConnection connection;
  int refreshTime = 180;

  boolean arrayCollectionEnabled = false;
  boolean arraySimpleValueEnabled = false;
  Map<String, SchemaVirtual> virtualCollections = new HashMap<>();

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

  public boolean isArrayCollectionEnabled() {
    return arrayCollectionEnabled;
  }

  public void setArrayCollectionEnabled(boolean arrayCollectionEnabled) {
    this.arrayCollectionEnabled = arrayCollectionEnabled;
  }

  public void init() {
    if (arrayCollectionEnabled || arraySimpleValueEnabled) {
      try {
        ArangoCursor<BaseDocument> cursor = connection.getDatabase().query("FOR c IN COLLECTIONS() FILTER !STARTS_WITH(c.name, '_') RETURN {name: c.name, schema: SCHEMA_GET(c.name)}", BaseDocument.class);
        if (cursor != null) {
          while (cursor.hasNext()) {
            BaseDocument doc = cursor.next();
            Map<String, Object> sa = (Map) doc.getAttribute("schema");
            if (sa != null) {
              String collection = (String) doc.getAttribute("name");
              logger.info("Schema for " + collection);
              String col = connection.getAliasCollection(collection);
              CollectionSchema schema = analyseSchema(collection, col, sa);
              if (!col.equals(collection))
                schemaMap.put(col, schema);
              schemaMap.put(collection, schema);
              if (schema.getReferences() != null) {
                for (SchemaNode sn : schema.getProperties()) {
                  if (sn.getDataType().contains(Types.ARRAY) && sn.getReferences() != null && !sn.getReferences().isEmpty()) {
                    String virtualCollection = collection + "_" + sn.getName();
                    virtualCollections.put(virtualCollection, new SchemaVirtual(collection, virtualCollection, sn.getName(), sn.isSimpleReferences()));
                  }
                }
              }
            }
          }
        }
      } catch (ArangoDBException e) {
        e.printStackTrace();
      }
    }
  }

  public boolean isArraySimpleValueEnabled() {
    return arraySimpleValueEnabled;
  }

  public void setArraySimpleValueEnabled(boolean arraySimpleValueEnabled) {
    this.arraySimpleValueEnabled = arraySimpleValueEnabled;
  }

  public CollectionSchema getSchema(String collection) {
    CollectionSchema schema = schemaMap.get(collection);
    if (schema == null || (schema.getNextRefresh() > 0 && schema.getNextRefresh() < System.currentTimeMillis())) {
      if (virtualCollections.containsKey(collection))
        return null;
      try {
        String col = connection.getAliasCollection(collection);
        ArangoCursor<BaseDocument> cursor = connection.getDatabase().query("RETURN SCHEMA_GET('" + col + "')", BaseDocument.class);
        if (cursor != null) {
          schema = analyseSchema(collection, col, cursor.next().getProperties());
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
      schema = analyseSchema(collection, col, docSchema.getProperties());
      if (!col.equals(collection))
        schemaMap.put(col, schema);
      schemaMap.put(collection, schema);
    }
    return schema;
  }

  private CollectionSchema analyseSchema(String collection, String col, Map<String, Object> doc) {
    Map<String, Object> rule = (Map) doc.get("rule");
    Map<String, Object> props = (Map) rule.get("properties");
    Map<String, Object> defs = (Map) rule.get("$defs");

    CollectionSchema schema = new CollectionSchema(col);
    if (!col.equals(collection))
      schema.setAliasName(collection);

    // all additional definition used by $ref
    if (defs != null) {
      HashMap<String, SchemaReference> refs = new HashMap<>();
      for (Map.Entry<String, Object> entry : defs.entrySet()) {
        String k = entry.getKey();
        Object v = entry.getValue();
        Map<String, Object> defProps = (Map) ((Map<String, Object>) v).get("properties");
        if (defProps != null)
          refs.put(k, new SchemaReference(k, createListOfNodes(defProps)));
        else {
          Map<String, Object> defValue = (Map) v;
          String mDt = (String) defValue.get("type");
          String mDf = (String) defValue.get("format");
          Number mMultipleOf = (Number) defValue.get("multipleOf");
          SchemaDatatype dt = new SchemaDatatype(k, findDataType(mDt, mDf, mMultipleOf));
          if (schema.getDatatypes() == null)
            schema.setDatatypes(new HashMap<>());
          schema.getDatatypes().put(k, dt);
        }
      }
      if (!refs.isEmpty())
        schema.setReferences(refs);
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
    for (Map.Entry<String, Object> entry : props.entrySet()) {
      String k = entry.getKey();
      Object v = entry.getValue();
      SchemaNode node = new SchemaNode(k);
      Map<String, Object> m = (Map) v;
      String dt = (String) m.get("type");
      String ref = (String) m.get("$ref");
      Map<String, Object> items = (Map<String, Object>) m.get("items");
      if (ref == null && items != null) {
        ref = (String) items.get("$ref");
      }
      String df = (String) m.get("format");
      List lstOneOf = (List) m.get("oneOf");
      List enumType = (List) m.get("enum");
      Number multipleOf = (Number) m.get("multipleOf");
      Object uProp = m.get("properties");

      if (ref != null) {
        ArrayList<Integer> dtList = new ArrayList<>();
        int dT = findDataType(dt, df, multipleOf);
        dtList.add(dT == Types.ARRAY ? dT : Types.STRUCT);
        node.setDataType(dtList);
        ArrayList<String> refList = new ArrayList<>();
        refList.add(ref.substring(ref.lastIndexOf('/') + 1));
        node.setReferences(refList);
      } else if (lstOneOf != null && lstOneOf.size() > 0) {
        ArrayList<Integer> dtList = new ArrayList<>();
        ArrayList<String> refList = new ArrayList<>();
        for (Object o : lstOneOf) {
          Map<String, Object> moo = (Map) o;
          String mDt = (String) moo.get("type");
          String mRef = (String) moo.get("$ref");
          Map<String, Object> mItems = (Map<String, Object>) moo.get("items");
          if (mRef == null && mItems != null) {
            mRef = (String) mItems.get("$ref");
          }
          String mDf = (String) moo.get("format");
          Number mMultipleOf = (Number) moo.get("multipleOf");
          if ("null".equalsIgnoreCase(mDt)) {
            node.setNullable(true);
          } else if (mRef != null) {
            int dT = findDataType(mDt, mDf, mMultipleOf);
            dtList.add(dT == Types.ARRAY ? dT : Types.STRUCT);
            refList.add(mRef.substring(mRef.lastIndexOf('/') + 1));
          } else {
            int dT = findDataType(mDt, mDf, mMultipleOf);
            dtList.add(dT);
            if (dT == Types.ARRAY && arraySimpleValueEnabled && mItems != null) {
              refList.add(mItems.get("type").toString());
              node.setSimpleReferences(true);
            }
          }
        }
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
        int dT = findDataType(dt, df, multipleOf);
        dtList.add(dT);
        if (dT == Types.ARRAY && arraySimpleValueEnabled && items != null) {
          ArrayList<String> refList = new ArrayList<>();
          refList.add(items.get("type").toString());
          node.setReferences(refList);
          node.setSimpleReferences(true);
        }
        node.setDataType(dtList);
      }
      if (uProp instanceof Map) {
        node.setProperties(createListOfNodes((Map<String, Object>) uProp));
      }
      nodes.add(node);
    }
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

  public HashMap<String, ColInfo> getColInfo(String collection) {
    HashMap<String, ColInfo> cols = new HashMap<>();

    CollectionSchema sm = getSchema(collection);
    if (sm != null) {
      for (SchemaNode sn : sm.getProperties()) {
        if (sn.getReferences() != null && !sn.getReferences().isEmpty()) {
          for (String ref : sn.getReferences()) {
            addRefCollInfo(sm, ref, sn.getName() + ".", cols);
          }
        } else {
          ColInfo ci = new ColInfo(sn.getName(), "NVARCHAR", sn.getDataType().get(0), String.class.getName());
          ci.tabName = sm.getAliasName();
          cols.put(ci.getName(), ci);
        }
      }
    }
    return cols;
  }

  private void addRefCollInfo(CollectionSchema sm, String ref, String prefix, HashMap<String, ColInfo> cols) {
    SchemaReference sr = sm.getReferences() != null ? sm.getReferences().get(ref) : null;
    if (sr != null) {
      for (SchemaNode sn : sr.getProperties()) {
        if (sn.getReferences() != null && !sn.getReferences().isEmpty()) {
          for (String r : sn.getReferences()) {
            addRefCollInfo(sm, r, prefix + sn.getName() + ".", cols);
          }
        } else {
          String name = prefix + sn.getName();
          ColInfo ci = new ColInfo(name, "NVARCHAR", sn.getDataType().get(0), String.class.getName());
          ci.tabName = sm.getAliasName();
          cols.put(name, ci);
        }
      }
    } else {
      SchemaDatatype dt = sm.getDatatypes() != null ? sm.getDatatypes().get(ref) : null;
      if (dt != null) {
        String name = prefix.substring(0, prefix.length() - 1);
        ColInfo ci = new ColInfo(name, "NVARCHAR", dt.getType(), String.class.getName());
        ci.tabName = sm.getAliasName();
        cols.put(name, ci);
      }
    }
  }

  public HashMap<String, ColInfo> getColInfo(String collection, String reference) {
    HashMap<String, ColInfo> cols = new HashMap<>();

    CollectionSchema sm = getSchema(collection);
    if (sm != null) {
      ColInfo ci = new ColInfo("_key", "NVARCHAR", Types.VARCHAR, String.class.getName());
      ci.tabName = sm.getAliasName();
      cols.put("_key", ci);
      addRefCollInfo(sm, reference, "", cols);
    }
    return cols;
  }

  public Map<String, SchemaVirtual> getVirtualCollections() {
    return virtualCollections;
  }

  public void setVirtualCollections(Map<String, SchemaVirtual> virtualCollections) {
    this.virtualCollections = virtualCollections;
  }
}
