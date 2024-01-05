package com.kafkaproject.libraryeventsproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaproject.libraryeventsproducer.Domain.LibraryEvent;
import com.kafkaproject.libraryeventsproducer.Domain.LibraryEventType;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.kafkaproject.libraryeventsproducer.producer.libraryEventProducer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class libraryEventsController {

    @Autowired
    private final libraryEventProducer libraryEventProducer;

    public libraryEventsController(com.kafkaproject.libraryeventsproducer.producer.libraryEventProducer libraryEventProducer) {
        this.libraryEventProducer = libraryEventProducer;
    }

    @PostMapping("/v1/libraryEvent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(
            @RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        log.info("library event : {}" + libraryEvent);
        // invoke kakfka producer
        libraryEventProducer.sendMessage(libraryEvent);
//libraryEventProducer.sendMessage_approach2(libraryEvent);
        log.info("After performing library event");

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryEventUpdate")
    public ResponseEntity<?> updateLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        log.info("Library Event :{}" + libraryEvent);
        ResponseEntity<String> BAD_REQUEST = validateRequest(libraryEvent);
        if (BAD_REQUEST != null) {
            return BAD_REQUEST;
        }
        libraryEventProducer.sendMessage_approach3(libraryEvent);

        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }

    @DeleteMapping("/v1/libraryEventDelete")
    public ResponseEntity<?> deleteEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        ResponseEntity<String> BAD_REQUEST = validateRequest(libraryEvent);
        if (BAD_REQUEST == null) {
            return BAD_REQUEST;
        }
        libraryEventProducer.sendMessage_approach3(libraryEvent);

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }


    private static ResponseEntity<String> validateRequest(LibraryEvent libraryEvent) {

        if(libraryEvent.libraryEventId()==null)
        {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass valid library event id");
        }

       else if(!libraryEvent.libraryEventType().equals(LibraryEventType.UPDATE))
        {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only update event type is supported....");
        }
        return null;

    }

}
