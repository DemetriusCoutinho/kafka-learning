package ecommerce;

import ecommerce.utils.KafkaProperties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static final String TOPIC = "ECOMMERCE_NEW_ORDER";
    private static final Logger log = LoggerFactory.getLogger(NewOrderMain.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var kafkaDispatcher = new KafkaDispatcher()) {
            var key = UUID.randomUUID().toString();
            var value = key + "123,1234";
            kafkaDispatcher.send(NewOrderMain.TOPIC, key, value);
            var email = "Thank you for your order! We are processing your order!";
            kafkaDispatcher.send(EmailService.TOPIC, key, email);
        }
    }

    public static void sendProducerEcommerce(ProducerRecord<String, String> record, KafkaProducer<String, String> kafkaProducer, Callback callback) throws ExecutionException, InterruptedException {
        kafkaProducer.send(record, callback).get();
    }
}
