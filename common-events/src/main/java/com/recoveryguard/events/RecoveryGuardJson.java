package com.recoveryguard.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * The single source of truth for how RecoveryGuard events are serialized.
 *
 * <p><b>Why this class exists.</b> Spring Kafka's default {@code JsonSerializer} builds its mapper
 * via {@code JacksonUtils.enhancedObjectMapper()}, which configures two features explicitly -- and
 * because {@code Jackson2ObjectMapperBuilder} only applies its default feature customization when
 * no features were set, {@code WRITE_DATES_AS_TIMESTAMPS} stays <em>enabled</em>. The measured
 * consequence on the previous code was:
 *
 * <pre>{@code "producedAt": 1790837640.123456789}</pre>
 *
 * <p>An epoch <em>decimal</em>. Any consumer that parses that as a JSON number -- Python's
 * {@code json}, JavaScript's {@code JSON.parse} -- receives a float64, which cannot represent
 * 1790837640.123456789 exactly (19 significant digits vs float64's ~15-17). Sub-microsecond
 * precision is silently discarded. For a project whose entire measurement plan is built on
 * detection, evidence-collection and validation latencies, a lossy timestamp is not a cosmetic
 * problem: it corrupts the recovery timeline that every verdict is justified by.
 *
 * <p>ISO-8601 strings are exact at any precision and unambiguous across languages, so that is what
 * this mapper emits. {@code EventSchemaTest} asserts the byte-level wire format so a future
 * dependency upgrade cannot quietly change it.
 */
public final class RecoveryGuardJson {

    private RecoveryGuardJson() {
    }

    /**
     * Builds the canonical RecoveryGuard {@link ObjectMapper}. Callers should create a new instance
     * rather than share one if they intend to mutate it.
     */
    public static ObjectMapper mapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                // Exact, language-neutral timestamps. See class javadoc.
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .enable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID)
                // Money is emitted at a fixed scale by the producers; plain notation avoids
                // scientific-notation surprises in downstream parsers.
                .enable(com.fasterxml.jackson.core.JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(MapperFeature.DEFAULT_VIEW_INCLUSION)
                // Keep the wire format stable: absent optional fields (e.g. a null causationId)
                // are omitted rather than emitted as explicit nulls.
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }
}
