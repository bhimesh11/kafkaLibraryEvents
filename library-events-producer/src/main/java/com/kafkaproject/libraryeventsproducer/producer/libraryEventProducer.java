package com.kafkaproject.libraryeventsproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaproject.libraryeventsproducer.Domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class libraryEventProducer
{

    @Value("${spring.kafka..topic}")
    public String topic;

    private final KafkaTemplate<Integer,String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public libraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<Integer,String>> sendMessage(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
        var CompletableFuture = kafkaTemplate.send(topic, key, value);
      return  CompletableFuture.whenComplete((SendResult, throwable) -> {

            if (throwable != null) {
                HandleError(key, value, throwable);
            } else {
                sendAlert(key, value, SendResult);
            }
        });

    }


    public SendResult<Integer,String> sendMessage_approach2(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
        var sendResult = kafkaTemplate.send(topic, key, value).get(3, TimeUnit.SECONDS);
       return sendAlert(key,value,sendResult);

    }
    public CompletableFuture<SendResult<Integer,String>> sendMessage_approach3(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
        var producerRecord = buildProducerRecord(key,value);
        var CompletableFuture = kafkaTemplate.send(producerRecord);
        return  CompletableFuture.whenComplete((SendResult, throwable) -> {

            if (throwable != null) {
                HandleError(key, value, throwable);
            } else {
                sendAlert(key, value, SendResult);
            }
        });

    }

    private ProducerRecord<Integer,String> buildProducerRecord(Integer key, String value) {

        List<Header> recordHeaders = List.of(new RecordHeader("event-source","scanner".getBytes()));
        return new ProducerRecord<>(topic,null,key,value);
    }

    private SendResult<Integer, String> sendAlert(Integer key, String value, SendResult<Integer, String> sendResult) {

        log.info("Message sent sucessfully for the key : {} and the value : {} , partition is {}",key,value,sendResult.getRecordMetadata().partition());
        return sendResult;
    }

    private void HandleError(Integer key, String value, Throwable throwable) {

        log.error("Error while send message to the toipic" + throwable.getMessage(),throwable);


    }
}
