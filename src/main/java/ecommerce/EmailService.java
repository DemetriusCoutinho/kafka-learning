package ecommerce;

import ecommerce.utils.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {
    public static final String TOPIC = "ECOMMERCE_SEND_EMAIL";

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList(EmailService.TOPIC));
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");
                records.forEach(record -> {
                            System.out.println("------------------------------------------------------------------");
                            System.out.println("Send email, checking for fraud ");
                            System.out.println(record.key());
                            System.out.println(record.value());
                            System.out.println(record.partition());
                            System.out.println(record.offset());
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException exception) {
                                //ignoring
                                exception.printStackTrace();
                            }
                            System.out.println("Email send");
                        }
                );
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.LOCAL_HOST);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());
//        properties.setProperty(ConsumerConfig)
//        properties.setProperty(ConsumerConfig)
        return properties;
    }
}
