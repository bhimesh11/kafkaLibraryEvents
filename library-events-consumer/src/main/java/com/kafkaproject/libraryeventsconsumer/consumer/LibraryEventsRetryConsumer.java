package com.kafkaproject.libraryeventsconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaproject.libraryeventsconsumer.service.libraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsRetryConsumer {

    @Autowired
    private libraryEventService libraryEventService;

    @KafkaListener(topics = {"${topics.retry}"},
            groupId = "retry-listener-group")
    public void onMessage(ConsumerRecord<Integer,String> consumerRecord)
    {
        log.info("ConsumerRecord in Retry consumer: {} " + consumerRecord);
        consumerRecord.headers()
                .forEach(header -> {
                    log.info("Key : {}, value :{}",header.key(),new String(header.value()));
                });
        try {
            libraryEventService.processLibraryEvent(consumerRecord);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
