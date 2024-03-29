package com.kafkaproject.libraryeventsconsumer.jpa;

import com.kafkaproject.libraryeventsconsumer.entity.Book;
import com.kafkaproject.libraryeventsconsumer.entity.LibraryEvent;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface LibraryEventRepository extends CrudRepository<LibraryEvent,Integer>
{

@Query("select b from Book b where b.bookId =:bookId ")
public Optional<Book> findByBookId(@Param("bookId") int bookid);

    @Transactional
    @Modifying
    @Query("DELETE FROM Book b WHERE b.bookId = :bookId")
    void deleteEventByBookId(@Param("bookId") int bookId);
}
