package com.brunas.ecommerce;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrder {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var dispatcher = new KafkaDispatcher()) {
            var key = UUID.randomUUID().toString();
            var value = key + " 22222, 333333333333"; //estou usando a mesma variavel para chave e valor
            dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);
            var email = "Welcome! We are processing you order";
            dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
        }
    }
}

