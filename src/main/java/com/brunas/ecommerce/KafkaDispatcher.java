package com.brunas.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, T> producer;

    KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

    //criando as propriedades
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //onde está rodando o kafka
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        return properties;
    }

    void send(String topic, String key, T value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, value); //(topico, chave, valor)
        //Callback(data, ex), que vai receber os dados de sucesso ou a exception de falha
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }

            System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };

        producer.send(record, callback).get();
    }

    @Override
    public void close() {
        producer.close();
    }
}





