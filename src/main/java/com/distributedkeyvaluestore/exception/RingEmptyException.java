package com.distributedkeyvaluestore.exception;

public class RingEmptyException extends RuntimeException {

    public RingEmptyException(String message) {
        super(message);
    }
}
