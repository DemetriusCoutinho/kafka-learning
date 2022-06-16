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
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static final String TOPIC = "ECOMMERCE_NEW_ORDER";
    private static final Logger log = LoggerFactory.getLogger(NewOrderMain.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());
        var value = "123,1234";
        var record = new ProducerRecord<>(TOPIC, value, value);
        var email = "Thank you for your order! We are processing your order!";
        var recordEmail = new ProducerRecord<>(EmailService.TOPIC, email, email);
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            log.info("Send Success: {} :: {} / {} / {} ", data.topic(), data.partition(), data.offset(), data.timestamp());
        };

        sendProducerEcommerce(record, producer, callback);
        sendProducerEcommerce(recordEmail, producer, callback);
    }

    public static void sendProducerEcommerce(ProducerRecord<String, String> record, KafkaProducer<String, String> kafkaProducer, Callback callback) throws ExecutionException, InterruptedException {
        kafkaProducer.send(record, callback).get();
    }

    public static void sendProducerEmail(ProducerRecord<String, String> record, KafkaProducer<String, String> kafkaProducer, Callback callback) throws ExecutionException, InterruptedException {
        kafkaProducer.send(record, callback).get();
    }


    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.LOCAL_HOST);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
