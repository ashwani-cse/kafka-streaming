package com.orders.domain;

public record OrderCountPerStoreDTO(String locationId,
                                    Long orderCount) {
}
