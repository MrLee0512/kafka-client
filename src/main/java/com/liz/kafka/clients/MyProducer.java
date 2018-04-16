package com.liz.kafka.clients;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer {

	public static void main(String[] args) {
		Properties properties = new Properties();

		properties.put("bootstrap.servers", "bigdata12:9092");
		properties.put("acks", "all");
		properties.put("retries", "0");
		properties.put("batch.size", "16384");
		properties.put("linger.ms", "1");
		properties.put("buffer.memory", "33554432");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = null;
		try {
			producer = new KafkaProducer<>(properties);
			int i = 0;
			while (true) {
				i ++;
				String msg = "消息：" + i;
				producer.send(new ProducerRecord<>("test", msg), (metadata, e) -> {
					if (e != null) {
						e.printStackTrace();
					}
					System.out.println("(offset: " + metadata.offset() + ", partition: "+ metadata.partition() +")");
				});
				Thread.sleep(2000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}

	}
}
