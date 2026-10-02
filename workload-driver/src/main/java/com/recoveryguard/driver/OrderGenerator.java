package com.recoveryguard.driver;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Deterministic order generation from a seed.
 *
 * <p>PRD section 20 lists "failure scenario is not reproducible" as a High-impact risk and prescribes
 * "deterministic workloads" as the mitigation. A workload whose contents differ between runs cannot
 * support a reproducibility claim, and cannot be compared across experiments -- so every field here
 * is derived from the seed and nothing from the clock, the environment, or {@code UUID.randomUUID()}.
 *
 * <p>Note the {@code orderId} itself is still minted by Order Service; determinism covers the
 * <em>business content</em>, which is what a validator needs in order to reconstruct what should
 * have happened.
 */
public final class OrderGenerator {

    /** Fixed SKU catalogue, so a given seed always selects the same products. */
    static final String[] SKUS = {
            "SKU-1001", "SKU-1002", "SKU-2001", "SKU-2002", "SKU-3001",
            "SKU-3002", "SKU-4001", "SKU-4002", "SKU-5001", "SKU-5002"
    };

    private final Random random;

    public OrderGenerator(long seed) {
        // java.util.Random with an explicit seed has a specified, stable algorithm, unlike
        // SplittableRandom whose output may change between JDK releases.
        this.random = new Random(seed);
    }

    /** One generated order line. */
    public record Line(String sku, int quantity, BigDecimal unitPrice) {
    }

    /** One generated order. */
    public record Order(String customerId, List<Line> lines, BigDecimal totalAmount) {
    }

    /** Generates the next order. Advances the internal state exactly once per call. */
    public Order next() {
        String customerId = "cust-" + (1000 + random.nextInt(9000));
        int lineCount = 1 + random.nextInt(4);
        List<Line> lines = new ArrayList<>(lineCount);
        BigDecimal total = BigDecimal.ZERO;
        for (int i = 0; i < lineCount; i++) {
            String sku = SKUS[random.nextInt(SKUS.length)];
            int quantity = 1 + random.nextInt(5);
            // Prices in whole cents, so serialized amounts are exact and stable.
            BigDecimal unitPrice = BigDecimal.valueOf(100 + random.nextInt(49_900))
                    .movePointLeft(2)
                    .setScale(2, RoundingMode.HALF_UP);
            lines.add(new Line(sku, quantity, unitPrice));
            total = total.add(unitPrice.multiply(BigDecimal.valueOf(quantity)));
        }
        return new Order(customerId, List.copyOf(lines), total.setScale(2, RoundingMode.HALF_UP));
    }

    /** Generates {@code count} orders as a single reproducible sequence. */
    public List<Order> generate(int count) {
        List<Order> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(next());
        }
        return List.copyOf(out);
    }
}
