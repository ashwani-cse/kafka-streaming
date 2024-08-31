package com.streams.management.order.util;

import com.orders.domain.OrderType;

import static com.streams.management.order.topology.OrdersTopology.GENERAL_ORDERS;
import static com.streams.management.order.topology.OrdersTopology.RESTAURANT_ORDERS;

public class CommonUtil {

    public static OrderType mapOrderType(String orderType){
        return switch (orderType) {
            case GENERAL_ORDERS -> OrderType.GENERAL;
            case RESTAURANT_ORDERS -> OrderType.RESTAURANT;
            default -> throw new IllegalStateException("Not a valid option");
        };
    }
}
