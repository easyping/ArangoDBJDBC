package de.hcbraun.arangodb.jdbc;

public class ColInfo implements Cloneable {
  String name, typeName, className, tabName = null;
  int type;

  ColInfo(String name, String typeName, int type, String className) {
    this.name = name;
    this.typeName = typeName;
    this.className = className;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getTabName() {
    return tabName;
  }

  public void setTabName(String tabName) {
    this.tabName = tabName;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
}
