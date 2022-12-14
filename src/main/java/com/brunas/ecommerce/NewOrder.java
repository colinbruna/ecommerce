package com.brunas.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrder {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            try (var emailDispatcher = new KafkaDispatcher<String>()) {
                var userId = UUID.randomUUID().toString();
                var orderId = UUID.randomUUID().toString();
                var amount = new BigDecimal(Math.random() * 500 + 1); //numero aleatório

                var order = new Order(userId, orderId, amount);
                orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

                var email = "Welcome! We are processing you order";
                emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
            }
        }
    }
}
