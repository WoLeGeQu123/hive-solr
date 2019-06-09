package net.ruixin.solr.exception;

public class SolrQueryException extends RuntimeException {
    public SolrQueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
