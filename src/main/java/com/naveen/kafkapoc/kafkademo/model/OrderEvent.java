package com.naveen.kafkapoc.kafkademo.model;

public record OrderEvent(String orderId, String product, int quantity, double price) {}
