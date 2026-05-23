package com.practice.dto;

import lombok.*;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter

public class UserPurchaseDTO {

    private String title;


    private String firstName;

    private String lastName;
}
