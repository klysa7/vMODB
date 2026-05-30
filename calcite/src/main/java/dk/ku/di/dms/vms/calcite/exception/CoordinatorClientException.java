package dk.ku.di.dms.vms.calcite.exception;

public final class CoordinatorClientException extends RuntimeException {
    public final String url;
    public final Integer statusCode;
    public final String body;

    public CoordinatorClientException(String message, String url, Integer statusCode, String body, Throwable cause) {
        super(message + " url=" + url + " status=" + statusCode, cause);
        this.url = url;
        this.statusCode = statusCode;
        this.body = body;
    }
}