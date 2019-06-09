package net.ruixin.solr;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;

/**
 * @author gai
 */
public class SolrField implements Serializable {
    /**
     * name: mandatory - the name for the field
     * type: mandatory - the name of a field type from the
     *   fieldTypes section
     * indexed: true if this field should be indexed (searchable or sortable)
     * stored: true if this field should be retrievable
     * multiValued: true if this field may contain multiple values per document
     */
    private String name;
    private String type;
    private Boolean stored=true;
    private Boolean indexed=true;
    private Boolean multiValued=false;
    public SolrField() {
    }
    public SolrField(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Boolean getStored() {
        return stored;
    }

    public void setStored(Boolean stored) {
        this.stored = stored;
    }

    public Boolean getIndexed() {
        return indexed;
    }

    public void setIndexed(Boolean indexed) {
        this.indexed = indexed;
    }

    public Boolean getMultiValued() {
        return multiValued;
    }

    public void setMultiValued(Boolean multiValued) {
        this.multiValued = multiValued;
    }

    public HashMap<String, Object> toHashMap(){
        HashMap<String, Object> fieldAttributes = new HashMap<>(5);
        fieldAttributes.put("name", this.getName());
        fieldAttributes.put("type", this.getType());
        fieldAttributes.put("indexed", this.getIndexed());
        fieldAttributes.put("stored", this.getStored());
        fieldAttributes.put("multiValued", this.getMultiValued());
        return fieldAttributes;
    }

    @Override
    public String toString() {
        return "SolrField{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", stored=" + stored +
                ", indexed=" + indexed +
                ", multiValued=" + multiValued +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SolrField solrField = (SolrField) o;
        return Objects.equals(name, solrField.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
