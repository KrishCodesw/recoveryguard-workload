package com.recoveryguard.order.web;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.util.List;

/**
 * Inbound order request.
 *
 * <p>Validation is not cosmetic here. An unvalidated request that produced a malformed event would
 * be a workload-side defect that RecoveryGuard could later misread as a recovery-side one -- the
 * same class of false positive as a dropped produce. Rejecting bad input at the boundary keeps the
 * only remaining explanation for a missing event the one the project is actually investigating.
 */
public record CreateOrderRequest(
        @NotBlank String customerId,
        @NotEmpty @Valid List<Line> lines
) {
    public record Line(
            @NotBlank String sku,
            @Positive int quantity,
            @NotNull @DecimalMin(value = "0.0", inclusive = false) BigDecimal unitPrice
    ) {
    }
}
