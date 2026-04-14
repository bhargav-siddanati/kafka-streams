package com.bhargav.fraud.detection.streams;

import com.bhargav.fraud.detection.model.Transaction;
import com.bhargav.fraud.detection.serde.TransactionSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Configuration
@Slf4j
public class KafkaStreamMethods {
    @Bean
    public KStream<String, Transaction> kafkaUsefulMethods(StreamsBuilder builder){
        KStream<String, Transaction> transactions = builder.stream("transactions", Consumed.with(Serdes.String(), new TransactionSerde()));

        /*transactions.peek((k,v) -> log.info("!!!!! ------- Another Bean ---------- !!!!!!"));*/
        /* Filter Method */
        transactions.filter((k,v) -> v.amount()>10000)
                .peek((k,v) -> log.info("-----------> !!!!!!######## This is Filter Method execution ########!!!!!! <-----------"));

        /* FilterNot Method */
        transactions.filterNot((k,v) -> v.amount() < 5000)
                .peek((k,v) -> log.info("-----------> !!!!!!######## This is FilterNot Method execution - whatever the condition not matched those will be logged ########!!!!!! <-----------"));

        /* Map Method */
        /**
         * Map method is used to transform each record in the stream. It takes a KeyValueMapper as an argument, which allows you to specify how to transform the key and value of each record. In this example, we are transforming the value to a string that summarizes the user's spending.
         */
        transactions.map((k,v) -> KeyValue.pair(v.userId(), "User Spent amount : " + v.amount()))
                .peek((k,v)->log.info("User Transaction Summary: Key={}, value={}", k, v));

        /* MapValue Method */
        transactions.mapValues(v -> "Transaction of amount " + v.amount() + " by user " + v.userId())
                .peek((k,v)->log.info("MapValue Result: Key={}, value={}", k, v));

        /* FlatMap Method and FlatMapValues Method */
        /*
        transactions.flatMap((k,v) -> {
            List<KeyValue<String, Item>> result = new ArrayList<>();
            for(Item item : v.items()){
                result.add(KeyValue.pair(v.userId(), item));
            }
            return result;
        }).peek((k,v) -> log.info("FlatMap Result: Key={}, value={}", k, v));
        //******************************  flatMapValues  *************************************
        transactions.flatMapValues(Transaction::items)
                .peek((k,v) -> log.info("FlatMapValues Result: Key={}, value={}", k, v));
        */

        /* Branch Method */
        // ---------------------------------------------------------
        /*
        KStream<String, Order> orders = ...;
                Map<String, KStream<String, Order>> branches = orders.split()
                .branch((k, v) -> v.amount > 1000, Branched.as("high-value"))
                .branch((k, v) -> v.amount > 100,  Branched.as("medium-value"))
                .defaultBranch(Branched.as("low-value"));

        // Access a specific branch
            branches.get("high-value").to("high-value-topic");

         */
        // -----------------------------------------------------------
        Map<String, KStream<String, Transaction>> stringKStreamMap = transactions.split()
                .branch((k, v) -> v.amount() > 10000, Branched.as("high-value"))
                .branch((k, v) -> v.amount() <= 10000, Branched.as("low-value"))
                .noDefaultBranch();

        // Process high-value transactions with terminal operation
        if(stringKStreamMap.get("high-value") != null) {
            stringKStreamMap.get("high-value")
                    .peek((key, value) -> System.out.println("High Value - Key: " + key + " Value: " + value))
                    .to("high-value-topic");
        }

        // Process low-value transactions with terminal operation
        if(stringKStreamMap.get("low-value") != null) {
            stringKStreamMap.get("low-value")
                    .peek((key, value) -> System.out.println("Low Value - Key: " + key + " Value: " + value))
                    .to("low-value-topic");
        }

        /* Group Method */
        transactions.groupBy((k,v) -> v.userId())
                .count()
                .toStream()
                .peek((k,v) -> log.info("GroupBy Result: Key={}, value={}", k, v));

        transactions.groupBy((k,v) -> v.userId())
                .count(Materialized.as("user-transaction-counts"))
                .toStream()
                .peek((k,v) -> log.info("GroupBy with Materialized Result: Key={}, value={}", k, v));

        transactions.groupBy((k,v) -> v.userId())
                .aggregate(() -> 0.0, (type, v, currSum) -> currSum + v.amount(), Materialized.as("user-transaction-sums"))
                .toStream()
                .peek((k,v) -> log.info("Aggregate Result: Key={}, value={}", k, v));

        return transactions;
    }
}
