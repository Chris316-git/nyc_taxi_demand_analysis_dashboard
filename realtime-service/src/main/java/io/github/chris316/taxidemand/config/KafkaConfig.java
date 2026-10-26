package io.github.chris316.taxidemand.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    NewTopic tripEventsTopic(AppProperties props) {
        return TopicBuilder.name(props.topic()).partitions(props.partitions()).replicas(1).build();
    }

    @Bean
    NewTopic tripEventsDeadLetterTopic(AppProperties props) {
        // Same partition count as the source topic: the recoverer publishes a failed record
        // to the DLT partition matching the one it came from.
        return TopicBuilder.name(props.topic() + ".DLT").partitions(props.partitions()).replicas(1).build();
    }

    /**
     * Batch listener with manual acks: offsets are committed only after the batch's
     * database transaction commits. A crash in between causes redelivery, which the
     * processed_trips table turns into a no-op (effectively-once counting).
     *
     * Transient failures (e.g. Postgres restarting) are retried with exponential backoff;
     * a batch that still fails after ~30s is published to the dead-letter topic so one
     * bad batch cannot stall a partition forever.
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaTemplate<Object, Object> template) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL);

        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxInterval(8_000L);
        backOff.setMaxElapsedTime(30_000L);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), backOff));
        return factory;
    }
}
