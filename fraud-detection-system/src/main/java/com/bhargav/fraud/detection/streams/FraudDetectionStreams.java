package com.bhargav.fraud.detection.streams;

import com.bhargav.fraud.detection.model.Transaction;
import com.bhargav.fraud.detection.serde.TransactionSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class FraudDetectionStreams {
    @Bean
    public KStream<String, Transaction> fraudDetection(StreamsBuilder streamsBuilder){

    KStream<String, Transaction> transactions =
        streamsBuilder.stream(
            "transactions", Consumed.with(Serdes.String(), new TransactionSerde()));

        transactions.filter((k,v)-> v.amount()>10000)
                .peek((k,v) -> log.warn("!!!!!!!!! Fraud transaction detected: Key={}, Value={}", k, v))
                .to("fraud-alert");

        return transactions;
    }
}
