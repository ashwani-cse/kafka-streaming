package com.orders.domain;

public record OrderRevenueDTO(
        String locationId,

        OrderType orderType,
        TotalRevenue totalRevenue
) {
}
