package com.practice.dto;

import lombok.*;

import java.time.Instant;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class TransactionDTO {

    private String uuid;

    private String name;

    private Long amount;

    private Instant time;

}
