package com.orders.domain;

public record Store(String locationId,
                    Address address,
                    String contactNum) {
}
