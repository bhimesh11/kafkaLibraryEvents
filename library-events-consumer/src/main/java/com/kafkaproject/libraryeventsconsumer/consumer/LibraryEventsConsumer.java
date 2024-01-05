package com.kafkaproject.libraryeventsconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import com.kafkaproject.libraryeventsconsumer.service.libraryEventService;

@Component
@Slf4j
public class LibraryEventsConsumer
{
    @Value("${spring.kafka.topic}")
    public String topic;

    @Autowired
    private libraryEventService libraryEventService;

    @KafkaListener(topics = {"library-events"} )
   public void onMessage(ConsumerRecord<Integer,String> consumerRecord)
   {
log.info("Consumer Record " + consumerRecord);
       try {
            libraryEventService.processLibraryEvent(consumerRecord);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
   }

//    @Override
//    @KafkaListener(topics = {"library-events"} )
//    public void onMessage(ConsumerRecord<Integer, String> data, Acknowledgment acknowledgment) {
//
//        log.info("Consumer Record " + data);
//        acknowledgment.acknowledge();
//        try {
//            libraryEventService.processLibraryEvent(data);
//        } catch (JsonProcessingException e) {
//            throw new RuntimeException(e);
//        }
//    }
}
