package com.heaven.kafkaproject;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerDemo {
	private String message;
	private String topic;

	public ProducerDemo(String topic, String message) {
		this.message = message;
		this.topic = topic;
	}

	public void sendData() {
		Producer<String, String> producer = new Producer<String, String>(ProducerConfig());
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(this.topic, message);
		producer.send(data);
		producer.close();
		System.out.println("producer send a message!" + message);
	}

	public static void main(String[] args) {
		new ProducerDemo("wanghouda", "wanghoudawanghouda").sendData();
	}

	public ProducerConfig ProducerConfig() {
		Properties props = new Properties();
		props.put("metadata.broker.list", "wanghouda:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("compression.codec", "snappy");
		props.put("producer.type", "async");
		props.put("request.required.acks", "1");
		props.put("send.buffer.bytes", "102400");
		return new ProducerConfig(props);

	}
}