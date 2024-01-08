package com.kafkaproject.libraryeventsconsumer.entity;


import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
@ToString
public class failureRecord
{
    @Id
    @GeneratedValue
private Integer id;
    private String topic;
    private Integer key_value;
    private String errorRecord;
    private Integer partition;
    private long offset_value;
    private String exception;
    private String status;


}
