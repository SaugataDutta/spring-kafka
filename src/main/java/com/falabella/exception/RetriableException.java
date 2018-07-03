package com.falabella.exception;

public class RetriableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private String message;

    public RetriableException(String message) {
        super(message);
        this.message = message;
    }
}
