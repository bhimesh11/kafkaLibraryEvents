package com.kafkaproject.libraryeventsproducer.Domain;

import com.kafkaproject.libraryeventsproducer.Domain.Book;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record LibraryEvent(
        Integer libraryEventId,

        LibraryEventType libraryEventType,

        @NotNull
        @Valid
        Book book
) {
}
