package com.kafkaproject.libraryeventsconsumer.service;

import com.kafkaproject.libraryeventsconsumer.entity.failureRecord;
import com.kafkaproject.libraryeventsconsumer.jpa.failureRecordRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class failureService {

    private failureRecordRepository failureRecordRepository;

    public failureService(com.kafkaproject.libraryeventsconsumer.jpa.failureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailObject(ConsumerRecord<Integer,String> record, Exception exception, String status)
    {
       var failureRecod = new failureRecord(null,record.topic(),
               record.key(),
                record.value(),
               record.partition(),
                record.offset(),
                exception.getCause().getMessage(),
                status);
       log.info(failureRecod.toString());
       failureRecordRepository.save(failureRecod);

    }
}

