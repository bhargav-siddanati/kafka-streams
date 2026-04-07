package com.bhargav.fraud.detection.model;

public record Transaction(String transactionId, String userId, double amount, String timeStamp) {}
