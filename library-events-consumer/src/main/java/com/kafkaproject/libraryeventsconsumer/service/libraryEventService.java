package com.kafkaproject.libraryeventsconsumer.service;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaproject.libraryeventsconsumer.entity.Book;
import com.kafkaproject.libraryeventsconsumer.entity.LibraryEvent;
import com.kafkaproject.libraryeventsconsumer.jpa.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class libraryEventService  {

    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    private LibraryEventRepository libraryEventRepository;
    public void processLibraryEvent(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {
     LibraryEvent libraryEvent =  objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
     log.info("Process Beginning..");
     log.info("Library Event {} ", libraryEvent);
     switch (libraryEvent.getLibraryEventType())
     {
         case NEW -> SaveObject(libraryEvent);
             //save operation;
         case UPDATE ->
             //update Operation
             updateObject(libraryEvent);

         case DELETE -> {
             if(libraryEvent.getLibraryEventId()!=null) {
                 deleteObject(libraryEvent,libraryEvent.getBook());
             }
         }
         default->
             log.info("Invalid library event type");
     }
    }

    private void deleteObject(LibraryEvent libraryEvent, Book book) {

        log.info("Process beginning for deleting the event");
        Optional<Book> bookId = libraryEventRepository.findByBookId(book.getBookId());
        if(bookId.isEmpty())
        {
            log.info("No record avaialble with give bookid");
        }
        else {
            libraryEventRepository.delete(libraryEvent  );

                log.info("Record delete sucessfully");
        }
        }

    private void updateObject(LibraryEvent libraryEvent)
    {
if(libraryEvent.getLibraryEventId()==null)
{
    throw new RuntimeException();
}
        Optional<LibraryEvent> check = libraryEventRepository.findById(libraryEvent.getLibraryEventId());
if(check==null)
{
    log.info("No Records feteched with given event id");
}
else {
    SaveObject(libraryEvent);
}
    }

    private void SaveObject(LibraryEvent libraryEvent) {

        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);
        log.info("Event stored in Database" + libraryEvent);
    }

}