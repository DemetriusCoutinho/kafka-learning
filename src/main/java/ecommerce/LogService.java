package ecommerce;

import ecommerce.utils.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {
    public static final String TOPIC_PATTERN = "ECOMMERCE.*";
    public static final Logger log = LoggerFactory.getLogger(LogService.class);

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Pattern.compile(TOPIC_PATTERN));
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");
                records.forEach(LogService::makeLog);
            }
        }
    }

    private static void makeLog(ConsumerRecord<String, String> consumerRecord) {
        System.out.println("------------------------------------------------------------------");
        String topicName = consumerRecord.topic();
        if (Objects.nonNull(topicName))
            log.info("LOG {}", topicName);
        else
            log.info("LOG {}", LogService.class.getSimpleName());
        log.info("{} INFO: RECORD KEY: {} || RECORD VALUE: {} || RECORD PARTITION: {} || RECORD OFFSET: {}",
                LogService.class.getSimpleName(),
                consumerRecord.key(), consumerRecord.value(),
                consumerRecord.partition(),
                consumerRecord.offset()
        );

    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.LOCAL_HOST);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());
        return properties;
    }
}
