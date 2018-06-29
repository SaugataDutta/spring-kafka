package com.falabella.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Builder
@AllArgsConstructor
@ToString
@NoArgsConstructor
@Getter
@Setter
public class Product {
    String title;
    String description;
}
