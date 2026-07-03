
package com.example.payment.dto;

import lombok.Builder;
import lombok.Data;@Data
@Builder
public class NotificationRequest {
    private String orderId;
    private String customerId;
    private String message;
    private String type;
}