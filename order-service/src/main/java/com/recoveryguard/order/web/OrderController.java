package com.recoveryguard.order.web;

import com.recoveryguard.order.service.OrderPublisher;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Order intake endpoint.
 *
 * <p>Two deliberate differences from the previous implementation:
 * <ul>
 *   <li>The wait for the broker's answer is <b>bounded</b>. Previously {@code .join()} blocked with
 *       no timeout, so during an injected failure a driver request could hang for up to
 *       {@code max.block.ms} + {@code delivery.timeout.ms} (~180s) and the experiment timeline would
 *       simply lose the event. The bound is enforced inside {@code DurablePublisher}, which resolves
 *       with {@code durable=false} instead of hanging.</li>
 *   <li>A non-durable outcome returns <b>503 with an explicit body</b> rather than a bare 500 from a
 *       {@code CompletionException}. The caller must be able to distinguish "rejected, never
 *       acknowledged" from "the service is broken".</li>
 * </ul>
 */
@RestController
@RequestMapping("/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final OrderPublisher orderPublisher;

    public OrderController(OrderPublisher orderPublisher) {
        this.orderPublisher = orderPublisher;
    }

    @PostMapping
    public ResponseEntity<OrderAckResponse> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        OrderAckResponse body = OrderAckResponse.from(orderPublisher.publishOrderCreated(request).join());
        if (body.durable()) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(body);
        }
        // 503 rather than 202: the order was NOT acknowledged and must not be treated as expected
        // state. The orderId is still returned so the caller can record the negative acknowledgement.
        log.warn("order {} was not acknowledged ({}); returning 503", body.orderId(), body.reason());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }
}
