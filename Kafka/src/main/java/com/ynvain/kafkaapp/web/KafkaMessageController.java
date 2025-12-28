package com.ynvain.kafkaapp.web;

import java.util.concurrent.CompletableFuture;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka")
public class KafkaMessageController {

  public KafkaMessageController(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  private final KafkaTemplate<String, String> kafkaTemplate;

  @PostMapping
  public void send(@RequestParam String message) {
    sendMessage(message);
  }

  public void sendMessage(String message) {
    CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("test", message);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        System.out.println(
            "Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset()
                + "]");
      } else {
        System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
      }
    });
  }
}
