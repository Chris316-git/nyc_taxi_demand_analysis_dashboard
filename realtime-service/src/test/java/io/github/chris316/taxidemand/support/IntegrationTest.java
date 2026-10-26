package io.github.chris316.taxidemand.support;

import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for integration tests against real Postgres, Kafka and Redis.
 *
 * <p>By default the three services run in Testcontainers, started once and shared by all
 * test classes. Set {@code -Dit.external=true} plus IT_POSTGRES_URL / IT_KAFKA_BOOTSTRAP /
 * IT_REDIS_HOST / IT_REDIS_PORT to run the same tests against services you started yourself.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class IntegrationTest {

    private static final boolean EXTERNAL = Boolean.getBoolean("it.external");

    static final PostgreSQLContainer<?> POSTGRES;
    static final KafkaContainer KAFKA;
    static final GenericContainer<?> REDIS;

    static {
        if (EXTERNAL) {
            POSTGRES = null;
            KAFKA = null;
            REDIS = null;
        } else {
            POSTGRES = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("taxi").withUsername("taxi").withPassword("taxi");
            KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));
            REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
            POSTGRES.start();
            KAFKA.start();
            REDIS.start();
        }
    }

    @DynamicPropertySource
    static void serviceProperties(DynamicPropertyRegistry registry) {
        if (EXTERNAL) {
            registry.add("spring.datasource.url", () -> System.getenv("IT_POSTGRES_URL"));
            registry.add("spring.datasource.username", () -> env("IT_POSTGRES_USER", "taxi"));
            registry.add("spring.datasource.password", () -> env("IT_POSTGRES_PASSWORD", "taxi"));
            registry.add("spring.kafka.bootstrap-servers", () -> env("IT_KAFKA_BOOTSTRAP", "localhost:9092"));
            registry.add("spring.data.redis.host", () -> env("IT_REDIS_HOST", "localhost"));
            registry.add("spring.data.redis.port", () -> env("IT_REDIS_PORT", "6379"));
        } else {
            registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
            registry.add("spring.datasource.username", POSTGRES::getUsername);
            registry.add("spring.datasource.password", POSTGRES::getPassword);
            registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
            registry.add("spring.data.redis.host", REDIS::getHost);
            registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        }
        // Small polls and fast commits keep the tests quick and deterministic.
        registry.add("spring.kafka.consumer.max-poll-records", () -> "200");
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    @Autowired
    protected JdbcTemplate jdbc;

    @Autowired
    protected StringRedisTemplate redis;

    @Autowired
    protected KafkaAdmin kafkaAdmin;

    @BeforeEach
    void resetState() {
        awaitConsumerCaughtUp();
        jdbc.execute("TRUNCATE processed_trips, zone_demand_buckets, partition_progress");
        jdbc.update("UPDATE dedup_horizon SET horizon = '1970-01-01T00:00:00Z' WHERE id = 1");
        redis.getConnectionFactory().getConnection().serverCommands().flushDb();
    }

    /**
     * Waits until the aggregator's committed offsets reach the end of every partition, so
     * records published by a previous test cannot land after this test resets the tables.
     */
    protected void awaitConsumerCaughtUp() {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> lag(admin) == 0);
        }
    }

    private static long lag(AdminClient admin) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = admin
                .listConsumerGroupOffsets("trip-demand-aggregator")
                .partitionsToOffsetAndMetadata().get();
        var partitions = admin.describeTopics(java.util.List.of("trip-events")).allTopicNames().get()
                .get("trip-events").partitions().stream()
                .map(p -> new TopicPartition("trip-events", p.partition()))
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = admin.listOffsets(partitions).all().get();
        long lag = 0;
        for (var e : ends.entrySet()) {
            OffsetAndMetadata c = committed.get(e.getKey());
            lag += e.getValue().offset() - (c == null ? 0 : c.offset());
        }
        return lag;
    }

    protected long totalTrips() {
        Long total = jdbc.queryForObject("SELECT COALESCE(SUM(trips), 0) FROM zone_demand_buckets", Long.class);
        return total == null ? 0 : total;
    }

    protected long processedIds() {
        Long n = jdbc.queryForObject("SELECT COUNT(*) FROM processed_trips", Long.class);
        return n == null ? 0 : n;
    }
}
