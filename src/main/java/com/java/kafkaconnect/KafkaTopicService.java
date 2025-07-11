package com.java.kafkaconnect;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Service
public class KafkaTopicService {

    @Value("${kafka.bootstrap.servers.dev}")
    private String bootstrapServersDev;

    @Value("${kafka.bootstrap.servers.local}")
    private String bootstrapServersLocal;

    @Value("${kafka.jaas.config}")
    private String jaasConfig;

    @Value("${kafka.dev.enabled}")
    private boolean isDevEnabled;

    private AdminClient adminClient;
    private KafkaProducer<String, String> producer;

    @PostConstruct
    public void init() {
        Properties props = isDevEnabled ? getDevAdminClientProperties() : getLocalAdminClientProperties();
        adminClient = AdminClient.create(props);

        // Producer properties
        Properties producerProps = isDevEnabled ? getDevProducerProperties() : getLocalProducerProperties();
        producer = new KafkaProducer<>(producerProps);
    }

    public Set<String> listTopics() throws ExecutionException, InterruptedException {
        ListTopicsResult topics = adminClient.listTopics();
        return topics.names().get();
    }

    public RecordMetadata sendEvent(String topic, String eventJson) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, eventJson);
        Future<RecordMetadata> future = producer.send(record);
        return future.get();
    }

    private Properties getDevAdminClientProperties() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersDev);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", jaasConfig);
        return props;
    }

    private Properties getLocalAdminClientProperties() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersLocal);
        props.put("security.protocol", "Plaintext");
        return props;
    }

    private Properties getLocalProducerProperties() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersLocal);
        producerProps.put("security.protocol", "Plaintext");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return producerProps;
    }

    private Properties getDevProducerProperties() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersDev);
        producerProps.put("security.protocol", "SASL_SSL");
        producerProps.put("sasl.mechanism", "PLAIN");
        producerProps.put("sasl.jaas.config", jaasConfig);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return producerProps;
    }
}
