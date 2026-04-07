package com.bhargav.fraud.detection.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {
    @Bean
    public NewTopic createTransactionTopic(){
        return new NewTopic("transactions", 3, (short)1);
    }
    @Bean
    public NewTopic createFraudTopic(){
        return new NewTopic("fraud-alert", 3, (short)1);
    }
}
