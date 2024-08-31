package com.orders.domain;

import java.time.LocalDateTime;

public record OrdersRevenuePerStoreByWindowsDTOP(String locationId,
                                                 TotalRevenue totalRevenue,
                                                 OrderType orderType,
                                                 LocalDateTime startWindow,
                                                 LocalDateTime endWindow) {
}
