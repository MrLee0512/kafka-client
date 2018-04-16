package com.liz.kafka.clients;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {
	private static Properties properties = new Properties();

	static {
		properties.put("bootstrap.servers", "bigdata12:9092");
		properties.put("group.id", "group-1");
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("auto.offset.reset", "earliest");
		properties.put("session.timeout.ms", "30000");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	}

	public void consumeTask(int taskId) {
		new Thread(() -> {
			KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
			kafkaConsumer.subscribe(Arrays.asList("test"));

			while (true) {
				ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("task = %d, partition = %d, offset = %d, value = %s", taskId, record.partition(), record.offset(), record.value());
					System.out.println();
				}
			}
		}).start();
	}

	public static void main(String[] args) {
		MyConsumer consumer = new MyConsumer();
		consumer.consumeTask(1);
		System.out.println("开启task：1");
		consumer.consumeTask(2);
		System.out.println("开启task：2");
	}

}
