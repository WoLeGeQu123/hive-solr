package net.ruixin.solr.exception;

public class SolrZkException extends RuntimeException {
    public SolrZkException() {
    }

    public SolrZkException(String message) {
        super(message);
    }

    public SolrZkException(String message, Throwable cause) {
        super(message, cause);
    }

    public SolrZkException(Throwable cause) {
        super(cause);
    }

    public SolrZkException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
