package com.bhargav.fraud.detection.streams;

import com.bhargav.fraud.detection.model.Transaction;
import com.bhargav.fraud.detection.serde.TransactionSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

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
    @Bean
    public KStream<String, Transaction> windowedTransactionStream(StreamsBuilder builder){
        KStream<String, Transaction> stream = builder.stream("Transactions", Consumed.with(Serdes.String(), new TransactionSerde()));
        stream.groupBy((k, tx) -> tx.userId(), Grouped.with(Serdes.String(), new TransactionSerde()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                .count(Materialized.as("user-txn-count-window-store"))
                .toStream()
                .peek((windowKey, count) -> {
                    String user = windowKey.key();
                    log.info("THIS IS THE WINDOW BASED METHOD");
                    log.info("User = {} | count = {}, window = [{} - {}]", user, count, windowKey.window().startTime(), windowKey.window().endTime());

                    if(count > 1 ){
                        log.info("Fraud alert : user: {} made {} transaction within 10 seconds!",
                                user, count);
                    }
                }).to("user-window-txn", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofSeconds(10).toMillis()), Serdes.Long()));
        return stream;
    }
}
