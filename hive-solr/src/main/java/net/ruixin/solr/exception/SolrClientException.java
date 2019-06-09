package net.ruixin.solr.exception;

public class SolrClientException extends RuntimeException {
    public SolrClientException() {
        super();
    }

    public SolrClientException(String message) {
        super(message);
    }

    public SolrClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public SolrClientException(Throwable cause) {
        super(cause);
    }

    public SolrClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
