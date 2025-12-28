package com.ynvain.kafkaapp.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaListenerService {

  @KafkaListener(topics = "test", groupId = "foo")
  public void listenGroupFoo(String message) {
    System.out.println("Received Message in group foo: " + message);
  }
}
