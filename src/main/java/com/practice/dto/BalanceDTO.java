package com.practice.dto;

import lombok.*;

import java.time.Instant;

import static java.util.Collections.max;

@Getter
@Setter
@Builder
@AllArgsConstructor
public class BalanceDTO {

    private Long balance;

    private Long count;

    private Instant lastTime;

    public BalanceDTO() {
        this.balance = 0L;
        this.count = 0L;
        this.lastTime = Instant.ofEpochMilli(0);
    }

    public void addTransaction(TransactionDTO transactionDTO) {
        this.balance += transactionDTO.getAmount();
        this.count++;
        this.lastTime = Instant.ofEpochMilli(Math.max(this.lastTime.toEpochMilli(), transactionDTO.getTime().toEpochMilli()));
    }
}
