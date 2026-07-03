package com.example.notification.domain;

public enum NotificationStatus {
    SENT,
    PARTIAL,  // Email or SMS failed but one channel succeeded
    FAILED
}
