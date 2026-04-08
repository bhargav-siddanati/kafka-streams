package com.bhargav.fraud.detection.serde;

import com.bhargav.fraud.detection.model.Transaction;
import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Custom deserializer for Transaction objects to convert JSON bytes from Kafka back into Transaction instances.
 */
public class TransactionDeserializer implements Deserializer<Transaction> {
    private final ObjectMapper mapper = new ObjectMapper();
    @Override
    public Transaction deserialize(String s, byte[] bytes) {
        try{
            return mapper.readValue(bytes, Transaction.class);
        }catch (Exception e){
            throw new RuntimeException("Error deserializing Transaction: " + e.getMessage(), e);
        }
    }
}
