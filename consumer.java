package com.mapr.kafka.serializer.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by Narmatha on 06/24/17.
 */
public class Consumer {
    private static Scanner in;

    public static void main(String[] argv)throws Exception{
        if (argv.length != 2) {
            System.err.printf("Usage: %s <topicName> <groupId>\n",
                    com.mapr.kafka.simple.Consumer.class.getSimpleName());
            System.exit(-1);
        }
        in = new Scanner(System.in);
        String topicName = argv[0];
        String groupId = argv[1];

        ConsumerThread consumerRunnable = new ConsumerThread(topicName,groupId);
        consumerRunnable.start();
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread{
        private String topicName;
        private String groupId;
        private KafkaConsumer<String,JsonNode> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId){
            this.topicName = topicName;
            this.groupId = groupId;
        }
        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");


            //Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, JsonNode>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));
            ObjectMapper mapper = new ObjectMapper();

            //Start processing messages
            try {
                while (true) {
                    ConsumerRecords<String, JsonNode> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, JsonNode> record : records) {
                        JsonNode jsonNode = record.value();
                        System.out.println(mapper.treeToValue(jsonNode,Contact.class));
                    }
                }
            }catch(WakeupException ex){
                System.out.println("Exception caught " + ex.getMessage());
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } finally{
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }
        public KafkaConsumer<String,JsonNode> getKafkaConsumer(){
            return this.kafkaConsumer;
        }
    }
}
