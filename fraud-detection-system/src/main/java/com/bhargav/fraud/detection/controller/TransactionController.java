package com.bhargav.fraud.detection.controller;

import com.bhargav.fraud.detection.model.Item;
import com.bhargav.fraud.detection.model.Transaction;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;

@RestController
@RequestMapping("/api/transactions")
@RequiredArgsConstructor
public class TransactionController {
    private final KafkaTemplate<String, Transaction> kafkaTemplate;

    @PostMapping
    public String sendTransaction() throws Exception{
        for(int i=0; i<10; i++){
            String transactionID = "txn_" + System.currentTimeMillis() + "-" + i;
            double amount = 8000 + new Random().nextDouble() * (11000 - 8000);
            Transaction txn;
            if(i%2!=0){
                Item item1 = new Item("Item:" + i, "Mobile");
                Item item2 = new Item("Item:" + i, "TV");
                Item item3 = new Item("Item:" + i, "AC");
                List<Item> itemList = List.of(item1, item2, item3);
                txn = new Transaction(transactionID, "USER_" + i, amount, LocalDateTime.now().toString(), "Credit Card", itemList);
            }else{
                Item item1 = new Item("Item:" + i, "Fridge");
                Item item2 = new Item("Item:" + i, "Table");
                Item item3 = new Item("Item:" + i, "Laptop");
                List<Item> itemList = List.of(item1, item2, item3);
                txn = new Transaction(transactionID, "USER_" + i, amount, LocalDateTime.now().toString(), "Debit Card", itemList);
            }
            kafkaTemplate.send("transactions", transactionID, txn);
        }
        return "Transaction sent to kafka";
    }
}
