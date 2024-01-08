package com.kafkaproject.libraryeventsconsumer.jpa;

import com.kafkaproject.libraryeventsconsumer.entity.failureRecord;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.ArrayList;

public interface failureRecordRepository extends CrudRepository<failureRecord,Integer>
{


    @Query("SELECT F FROM failureRecord F WHERE F.status = :retry")
    ArrayList<failureRecord> findAllByStatus(@Param("retry") String retry);
}
