package com.recoveryguard.driver;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Determinism is the property that makes an experiment reproducible rather than merely re-runnable
 * (PRD section 20 names non-reproducible scenarios a High-impact risk).
 */
class OrderGeneratorTest {

    @Test
    @DisplayName("the same seed produces a byte-identical sequence")
    void sameSeedSameSequence() {
        List<OrderGenerator.Order> a = new OrderGenerator(42L).generate(500);
        List<OrderGenerator.Order> b = new OrderGenerator(42L).generate(500);
        assertThat(a).isEqualTo(b);
    }

    @Test
    @DisplayName("different seeds produce different sequences")
    void differentSeedsDiffer() {
        assertThat(new OrderGenerator(1L).generate(50))
                .isNotEqualTo(new OrderGenerator(2L).generate(50));
    }

    @Test
    @DisplayName("streaming one-at-a-time matches the batch sequence")
    void streamingMatchesBatch() {
        OrderGenerator streaming = new OrderGenerator(7L);
        List<OrderGenerator.Order> batch = new OrderGenerator(7L).generate(100);
        for (int i = 0; i < 100; i++) {
            assertThat(streaming.next()).isEqualTo(batch.get(i));
        }
    }

    @Test
    @DisplayName("totals are exact, scaled to cents, and consistent with the lines")
    void totalsAreExact() {
        for (OrderGenerator.Order o : new OrderGenerator(99L).generate(1000)) {
            assertThat(o.totalAmount().scale()).isEqualTo(2);
            BigDecimal recomputed = o.lines().stream()
                    .map(l -> l.unitPrice().multiply(BigDecimal.valueOf(l.quantity())))
                    .reduce(BigDecimal.ZERO, BigDecimal::add)
                    .setScale(2, RoundingMode.HALF_UP);
            assertThat(o.totalAmount()).isEqualByComparingTo(recomputed);
            assertThat(o.lines()).isNotEmpty();
            assertThat(o.customerId()).startsWith("cust-");
            o.lines().forEach(l -> {
                assertThat(l.quantity()).isPositive();
                assertThat(l.unitPrice().scale()).isEqualTo(2);
                assertThat((Object) l.sku()).isIn((Object[]) OrderGenerator.SKUS);
            });
        }
    }
}
