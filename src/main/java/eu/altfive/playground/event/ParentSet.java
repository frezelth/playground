package eu.altfive.playground.event;

public class ParentSet {
  private String id;
  private String parentId;

  public ParentSet() {
  }

  public ParentSet(String id, String parentId) {
    this.id = id;
    this.parentId = parentId;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getParentId() {
    return parentId;
  }

  public void setParentId(String parentId) {
    this.parentId = parentId;
  }
}