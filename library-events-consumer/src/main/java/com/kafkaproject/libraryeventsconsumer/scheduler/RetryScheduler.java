package com.kafkaproject.libraryeventsconsumer.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaproject.libraryeventsconsumer.GenericConstants.genericConstants;
import com.kafkaproject.libraryeventsconsumer.config.LibraryEventsConsumerConfig;
import com.kafkaproject.libraryeventsconsumer.entity.failureRecord;
import com.kafkaproject.libraryeventsconsumer.jpa.LibraryEventRepository;
import com.kafkaproject.libraryeventsconsumer.jpa.failureRecordRepository;
import com.kafkaproject.libraryeventsconsumer.service.libraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryScheduler
{
    @Autowired
    private failureRecordRepository failureRecordRepository;
    @Autowired
    private LibraryEventRepository libraryEventRepository;
    @Autowired
    private LibraryEventsConsumerConfig libraryEventsConsumerConfig;
    @Autowired
    private genericConstants values;
    @Autowired
    private libraryEventService libraryEventService;


    @Scheduled(fixedRate = 10000)
    public void retryFailedRecord()
    {
        log.info("process beginning for retriving the failed record ");
        failureRecordRepository.findAllByStatus(values.RETRY)
                .forEach(failureRecord -> {
                    log.info("Building consumer record from failureRecord");

                  var failedConsumerRecord = buildConsumerRecord(failureRecord);
                    try {
                        libraryEventService.processLibraryEvent(failedConsumerRecord);
                        log.info("Persisted");
                        failureRecord.setStatus(values.SUCESS);
                        failureRecordRepository.save(failureRecord);

                    } catch (JsonProcessingException e) {
                        log.error("Exception again from retry scheduler"+e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

    }

    private ConsumerRecord<Integer,String> buildConsumerRecord(failureRecord failureRecord) {
          return new ConsumerRecord<Integer,String>(
                  failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffset_value(),
                failureRecord.getKey_value(),
                failureRecord.getErrorRecord()
          );
    }

}
