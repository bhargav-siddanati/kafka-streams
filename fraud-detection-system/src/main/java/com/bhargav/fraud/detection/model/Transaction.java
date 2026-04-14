package com.bhargav.fraud.detection.model;

import java.util.List;

public record Transaction(String transactionId, String userId, double amount, String timeStamp, String type, List<Item> item) {}
