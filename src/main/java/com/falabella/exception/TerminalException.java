package com.falabella.exception;

public class TerminalException extends RuntimeException{
    private static final long serialVersionUID = 1L;

    private String message;

    public TerminalException(String message) {
        super(message);
        this.message = message;
    }
}
