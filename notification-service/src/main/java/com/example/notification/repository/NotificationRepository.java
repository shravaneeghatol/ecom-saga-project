package com.example.notification.repository;

import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface NotificationRepository extends JpaRepository<Notification, UUID> {
    Optional<Notification> findByOrderIdAndStatus(String orderId, NotificationStatus status);

    long countByStatus(NotificationStatus status);
}