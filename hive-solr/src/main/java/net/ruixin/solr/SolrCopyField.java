package net.ruixin.solr;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author gai
 */
public class SolrCopyField implements Serializable {
    private String source;
    private String dest;
    public SolrCopyField(){}
    public SolrCopyField(String source, String dest) {
        this.source = source;
        this.dest = dest;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    @Override
    public String toString() {
        return "SolrCopyField{" +
                "source='" + source + '\'' +
                ", dest='" + dest + '\'' +
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
        SolrCopyField that = (SolrCopyField) o;
        return Objects.equals(source, that.source) &&
                Objects.equals(dest, that.dest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, dest);
    }
}
