package com.java.kafkaconnect;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.MediaType;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/kafka")
public class KafkaTopicController {

    @Autowired
    private KafkaTopicService kafkaTopicService;

    @GetMapping("/topics")
    public ResponseEntity<?> listTopics() {
        try {
            Set<String> topics = kafkaTopicService.listTopics();
            return ResponseEntity.ok(topics);
        } catch (ExecutionException | InterruptedException e) {
            return ResponseEntity.status(500).body("Error listing topics: " + e.getMessage());
        }
    }

    @PostMapping(value = "/event", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> postEvent(@RequestParam String topic, @RequestBody Map<String, Object> event) {
        try {
            String eventJson = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(event);
            kafkaTopicService.sendEvent(topic, eventJson);
            return ResponseEntity.ok("Event sent to topic: " + topic);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error sending event: " + e.getMessage());
        }
    }
}
