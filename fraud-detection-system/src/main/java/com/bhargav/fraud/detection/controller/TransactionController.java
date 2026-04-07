package com.bhargav.fraud.detection.controller;

import com.bhargav.fraud.detection.model.Transaction;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.Random;

@RestController
@RequestMapping("/api/transactions")
@RequiredArgsConstructor
public class TransactionController {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostMapping
    public String sendTransaction() throws Exception{
        for(int i=0; i<10; i++){
            String transactionID = "txn_" + System.currentTimeMillis() + "-" + i;
            double amount = 8000 + new Random().nextDouble() * (11000 - 8000);
            Transaction txn = new Transaction(transactionID, "USER_" + i, amount, LocalDateTime.now().toString());
            String txnJson = objectMapper.writeValueAsString(txn);
            kafkaTemplate.send("transactions", transactionID, txnJson);
        }
        return "Transaction sent to kafka";
    }
}
