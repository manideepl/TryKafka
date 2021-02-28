package com.github.manideepl.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class ProducerWithCallbackDemo {
  public static void main(String[] args) {

    Logger logger = LoggerFactory.getLogger(ProducerWithCallbackDemo.class.getName());
    //create properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //create producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


    //send
      //create producer record
      ProducerRecord<String, String> harry = new ProducerRecord<String, String>("hogwarts", "Gryffindor", "Harry");
    ProducerRecord<String, String> ron = new ProducerRecord<String, String>("hogwarts", "Gryffindor", "Ron");
    ProducerRecord<String, String> hermione = new ProducerRecord<String, String>("hogwarts", "Gryffindor", "Hermione");
    ProducerRecord<String, String> draco = new ProducerRecord<String, String>("hogwarts", "Slytherin", "Draco");
    ProducerRecord<String, String> crabbe = new ProducerRecord<String, String>("hogwarts", "Slytherin", "Crabbe");
    ProducerRecord<String, String> goyle = new ProducerRecord<String, String>("hogwarts", "Slytherin", "Goyle");
    ProducerRecord<String, String> luna = new ProducerRecord<String, String>("hogwarts", "Ravenclaw", "Luna");
    ProducerRecord<String, String> ernie = new ProducerRecord<String, String>("hogwarts", "Hufflepuff", "Ernie");
    ProducerRecord<String, String> manideep = new ProducerRecord<String, String>("hogwarts", "Mutants", "Manideep");


     List<ProducerRecord<String, String>> students = Arrays.asList(harry, ron, hermione, draco, crabbe, goyle, luna, ernie, manideep);

     students.forEach(record -> {
      producer.send(record, (metadata, exception) -> {
        if (exception == null) {

          logger.info("metadata-topic: " + metadata.topic() + "\n" + "metadata:offset" + metadata.offset() + "\n" + "metadata:partition" + metadata.partition() + "\n" + "message: " + record.value());
        } else {
          logger.error(exception.getMessage());
        }
      });
     });

    producer.flush();
    producer.close();

  }
}
