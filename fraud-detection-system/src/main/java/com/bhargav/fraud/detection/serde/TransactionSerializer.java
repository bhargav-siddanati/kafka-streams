package com.bhargav.fraud.detection.serde;

import com.bhargav.fraud.detection.model.Transaction;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Custom serializer for Transaction objects to convert them to JSON bytes for Kafka.
 */
public class TransactionSerializer implements Serializer<Transaction> {
    private final ObjectMapper mapper = new ObjectMapper();
    @Override
    public byte[] serialize(String s, Transaction transaction) {
        try{
            return mapper.writeValueAsBytes(transaction);
        }catch (Exception e){
            throw new SerializationException("Error serializing Transaction: " + e.getMessage(), e);
        }
    }
}
