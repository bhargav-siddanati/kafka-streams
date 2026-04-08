package com.bhargav.fraud.detection.serde;

import com.bhargav.fraud.detection.model.Transaction;
import org.apache.kafka.common.serialization.Serdes;

public class TransactionSerde extends Serdes.WrapperSerde<Transaction> {
    public TransactionSerde() {
    super(new TransactionSerializer(), new TransactionDeserializer());
    }
}
