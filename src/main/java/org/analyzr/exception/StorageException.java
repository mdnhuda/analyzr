package org.analyzr.exception;

/**
 * @author naimulhuda
 * @since 3/10/2016
 */
public class StorageException extends RuntimeException {
    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageException(String message) {
        super(message);
    }
}
