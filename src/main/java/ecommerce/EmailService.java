package ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {
    public static final String TOPIC = "ECOMMERCE_SEND_EMAIL";

    public static void main(String[] args) {
        var emailService = new EmailService();
       try(var service = new KafkaService(EmailService.class.getSimpleName(), EmailService.TOPIC, emailService::parse)){
        service.run();}
    }

    private void parse(ConsumerRecord<String, String> record) {
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


}
