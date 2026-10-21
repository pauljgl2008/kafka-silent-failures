package dev.paulguevara.silentfailures.payments;

/** A message that can never be processed, no matter how many times we retry it. */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message) {
        super(message);
    }

    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
