package com.example.inventory.exception;

public class OutboxPublishException extends RuntimeException {
    public OutboxPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}