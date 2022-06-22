package ecommerce;

import ecommerce.utils.KafkaProperties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher implements Closeable {
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaDispatcher.class);
    private final KafkaProducer<String, String> producer;

    public KafkaDispatcher() {
        this.producer = new KafkaProducer<String, String>(properties());
    }

    private Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.LOCAL_HOST);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    public void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, value);
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            LOGGER.info("Send Success: {} :: {} / {} / {} ", data.topic(), data.partition(), data.offset(), data.timestamp());
        };
        producer.send(record, callback).get();
    }

    @Override
    public void close() {
        this.producer.close();
    }
}
