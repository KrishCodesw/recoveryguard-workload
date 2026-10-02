package com.recoveryguard.events.spring;

import com.recoveryguard.events.AckRecord;
import com.recoveryguard.events.AcknowledgementLedger;
import com.recoveryguard.events.ClusterContractGuard;
import com.recoveryguard.events.DurablePublisher;
import com.recoveryguard.events.KafkaContract;
import com.recoveryguard.events.ListenerErrorSupport;
import com.recoveryguard.events.RecoveryGuardJson;
import com.recoveryguard.events.TopicContract;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Shared workload infrastructure, auto-configured into every service.
 *
 * <p>Provides the four things that must be identical across services for RecoveryGuard's evidence to
 * be comparable: the acknowledged-event contract settings, the acknowledgement ledger, explicit topic
 * declarations, and the startup cluster-contract guard.
 *
 * <p>Kept in {@code common-events} behind optional Spring dependencies so the event schemas and
 * ledger remain usable from non-JVM tools, while the three services need no duplicated wiring.
 * Every bean is {@code @ConditionalOnMissingBean}, so a service can override any of it.
 */
@AutoConfiguration
@ConditionalOnClass({KafkaTemplate.class, AcknowledgementLedger.class})
@EnableConfigurationProperties(RecoveryGuardProperties.class)
public class RecoveryGuardWorkloadAutoConfiguration {

    private static final Logger log =
            LoggerFactory.getLogger(RecoveryGuardWorkloadAutoConfiguration.class);

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.application.name:workload-service}")
    private String serviceName;

    /**
     * The ground-truth acknowledgement ledger -- the artifact that makes RecoveryGuard's central
     * check non-circular. Expected state must be recorded independently of Kafka, or the broker
     * failure that loses an event also loses the evidence that it existed.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public AcknowledgementLedger acknowledgementLedger(RecoveryGuardProperties props) {
        String exp = (props.getExperimentId() == null || props.getExperimentId().isBlank())
                ? AckRecord.UNSCOPED_EXPERIMENT : props.getExperimentId();
        Path dir = Path.of(props.getLedger().getPath(), exp);
        AcknowledgementLedger ledger =
                new AcknowledgementLedger(dir, serviceName, exp, props.getLedger().isFsync());
        log.info("acknowledgement ledger -> {} (fsync={}, experiment={})",
                ledger.file(), props.getLedger().isFsync(), exp);
        return ledger;
    }

    @Bean
    @ConditionalOnMissingBean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = KafkaContract.producerProps(bootstrapServers);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
        // Pin the wire format: JsonSerializer's no-arg constructor builds its own mapper with
        // WRITE_DATES_AS_TIMESTAMPS enabled, which emitted lossy epoch-decimal timestamps.
        factory.setValueSerializer(new JsonSerializer<>(RecoveryGuardJson.mapper()));
        return factory;
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    @ConditionalOnMissingBean
    public DurablePublisher durablePublisher(KafkaTemplate<String, Object> kafkaTemplate,
                                             AcknowledgementLedger ledger,
                                             RecoveryGuardProperties props) {
        return new DurablePublisher(kafkaTemplate, ledger,
                Duration.ofMillis(props.getProducer().getAckTimeoutMs()));
    }

    /**
     * Routes exhausted records to {@code <topic>.DLT} and records the abandonment in the ledger.
     * Without this, Spring's default error handler logs and skips -- silent, unattributable loss
     * inside the workload that RecoveryGuard would otherwise report as a data-integrity failure.
     */
    @Bean
    @ConditionalOnMissingBean
    public CommonErrorHandler recoveryGuardErrorHandler(KafkaTemplate<String, Object> kafkaTemplate,
                                                        AcknowledgementLedger ledger,
                                                        RecoveryGuardProperties props) {
        return ListenerErrorSupport.errorHandler(kafkaTemplate, ledger,
                props.getListener().getRetryIntervalMs(), props.getListener().getMaxAttempts());
    }

    /**
     * Declares the Phase 1 topics and their DLTs explicitly, with a replication factor and
     * {@code min.insync.replicas} that make {@code acks=all} mean something. Relied-on broker
     * auto-create previously produced single-partition, RF-1 topics, where neither partition-targeted
     * fault injection nor any replication-based failure mode is expressible.
     */
    @Bean
    @ConditionalOnMissingBean
    public KafkaAdmin.NewTopics recoveryGuardTopics(RecoveryGuardProperties props) {
        boolean dev = isDevProfile(props);
        List<TopicContract> contracts = TopicContract.forProfile(dev);
        List<NewTopic> topics = new ArrayList<>();
        for (TopicContract tc : contracts) {
            var builder = TopicBuilder.name(tc.name())
                    .partitions(tc.partitions())
                    .replicas(tc.replicationFactor());
            tc.toKafkaConfigs().forEach(builder::config);
            topics.add(builder.build());
        }
        log.info("declaring {} topics (profile={}): {}", topics.size(),
                props.getCluster().getProfile(), topics.stream().map(NewTopic::name).toList());
        // KafkaAdmin discovers KafkaAdmin.NewTopics beans; a raw List<NewTopic> bean is ignored.
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }

    /**
     * Verifies at startup that the attached cluster can honour the acknowledged-event contract.
     *
     * <p>Under {@code experiment} a violation aborts startup: on a cluster where {@code acks=all}
     * silently degrades to {@code acks=1}, a {@code DATA_INTEGRITY_FAILURE} verdict would be
     * indistinguishable from one caused by a real recovery defect. Under {@code dev} it warns, so
     * single-broker local iteration still works -- but loudly, so nobody mistakes a dev cluster for
     * a valid experiment target.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "recoveryguard.cluster", name = "guard-enabled",
            havingValue = "true", matchIfMissing = true)
    public ClusterContractGuard.Report clusterContractReport(RecoveryGuardProperties props,
                                                             AcknowledgementLedger ledger) {
        boolean dev = isDevProfile(props);
        var profile = dev ? ClusterContractGuard.Profile.DEV : ClusterContractGuard.Profile.EXPERIMENT;
        ClusterContractGuard.Report report = ClusterContractGuard.require(
                bootstrapServers, profile, TopicContract.forProfile(dev),
                Duration.ofMillis(props.getCluster().getGuardTimeoutMs()));
        if (report.compliant()) {
            log.info("cluster contract satisfied ({} broker(s), clusterId={})",
                    report.brokerCount(), report.clusterId());
        } else {
            log.warn("cluster contract NOT satisfied -- results from this cluster are not valid "
                    + "evidence for any RecoveryGuard verdict:\n{}", report.render());
        }
        if (!ledger.healthy()) {
            log.warn("acknowledgement ledger is unhealthy ({}); experiments should be treated as "
                    + "INCONCLUSIVE", ledger.lastError());
        }
        return report;
    }

    private static boolean isDevProfile(RecoveryGuardProperties props) {
        return !"experiment".equalsIgnoreCase(props.getCluster().getProfile());
    }
}
