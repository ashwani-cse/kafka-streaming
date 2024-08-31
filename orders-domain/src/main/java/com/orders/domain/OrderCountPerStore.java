package com.orders.domain;

public record OrderCountPerStore(String locationId,
                                 Long orderCount) {
}
