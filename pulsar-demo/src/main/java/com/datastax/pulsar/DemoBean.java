package com.datastax.pulsar;

public class DemoBean {
    String id;
    String value;
    
    public DemoBean(String id, String value) {
        super();
        this.id = id;
        this.value = value;
    }
    /**
     * Getter accessor for attribute 'id'.
     *
     * @return
     *       current value of 'id'
     */
    public String getId() {
        return id;
    }
    /**
     * Setter accessor for attribute 'id'.
     * @param id
     *      new value for 'id '
     */
    public void setId(String id) {
        this.id = id;
    }
    /**
     * Getter accessor for attribute 'value'.
     *
     * @return
     *       current value of 'value'
     */
    public String getValue() {
        return value;
    }
    /**
     * Setter accessor for attribute 'value'.
     * @param value
     *      new value for 'value '
     */
    public void setValue(String value) {
        this.value = value;
    }
}
