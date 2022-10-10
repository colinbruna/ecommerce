package com.brunas.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrder {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //produzindo uma mensagem
        var producer = new KafkaProducer<String, String>(properties()); //<tipo da chave, tipo da mensagem>(propriedades)
        var key = UUID.randomUUID().toString();

        //Callback(data, ex), que vai receber os dados de sucesso ou a exception de falha
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }

            System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };

        var value = key + " 22222, 333333333333"; //estou usando a mesma variavel para chave e valor
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value); //(topico, chave, valor)

        var email = "Welcome! We are processing you order";
        var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);

        //enviando a mensagem (a mensagem é um record)
        producer.send(record, callback).get();

        producer.send(emailRecord,callback).get();
    }

    //criando as propriedades
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //onde está rodando o kafka
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
