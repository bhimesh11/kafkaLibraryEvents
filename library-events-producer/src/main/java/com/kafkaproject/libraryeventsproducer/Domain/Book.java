package com.kafkaproject.libraryeventsproducer.Domain;


import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record Book(
        Integer bookId,
        String bookName,

        String bookAuthor


) {
}
