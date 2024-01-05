import com.kafkaproject.libraryeventsproducer.Domain.LibraryEvent;
import com.kafkaproject.libraryeventsproducer.controller.libraryEventsController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
//import unit.com.learnkafka.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = libraryEventsController.class,webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class libraryEventsControllerTest {

    @Autowired
    TestRestTemplate restTemplate;

    @Test
    void postLibraryEvent()
    {
        //given
        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
       // HttpEntity<LibraryEvent> request = new HttpEntity<>(Lib)
        var httpEntity = new HttpEntity<>(unit.com.learnkafka.util.TestUtil.libraryEventRecord(),headers);

        //when
        var responseEntity = restTemplate.exchange("/v1/libraryEvent", HttpMethod.POST,httpEntity,LibraryEvent.class);

        //then

        assertEquals(HttpStatus.CREATED,responseEntity.getStatusCode());
    }
}