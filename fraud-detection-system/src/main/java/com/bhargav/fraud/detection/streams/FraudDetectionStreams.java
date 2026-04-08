package com.bhargav.fraud.detection.streams;

import com.bhargav.fraud.detection.model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.ObjectMapper;

@Configuration
@EnableKafkaStreams
@Slf4j
public class FraudDetectionStreams {
    @Bean
    public KStream<String, Transaction> fraudDetection(StreamsBuilder streamsBuilder){
        var transactionSerde = new JacksonJsonSerde<>(Transaction.class);

        KStream<String, Transaction> transactions =
            streamsBuilder.stream("transactions", Consumed.with(Serdes.String(), transactionSerde));

        transactions.filter((k,v)-> v.amount()>10000)
                .peek((k,v) -> log.warn("!!!!!!!!! Fraud transaction detected: Key={}, Value={}", k, v))
                .to("fraud-alert");

       /* KStream<String, Transaction> fraudTransaction = transactions.filter((k, v) -> isSuspiciousTransaction(v))
                .peek((k, v) -> log.warn("Fraudulent transaction detected: Key={}, Value={}", k, v));

        fraudTransaction.to("fraud-alert");*/

        return transactions;


    }

    private boolean isSuspiciousTransaction(String value){
        Transaction transaction = new ObjectMapper().readValue(value, Transaction.class);
        return transaction.amount() > 10000;
    }
}
