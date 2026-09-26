package com.recoveryguard.order.web;

import java.math.BigDecimal;
import java.util.List;

public record CreateOrderRequest(
        String customerId,
        List<Line> lines
) {
    public record Line(String sku, int quantity, BigDecimal unitPrice) {
    }
}
