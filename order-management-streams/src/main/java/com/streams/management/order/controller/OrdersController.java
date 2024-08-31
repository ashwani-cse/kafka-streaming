package com.streams.management.order.controller;

import com.orders.domain.AllOrdersCountPerStoreDTO;
import com.orders.domain.OrderCountPerStoreDTO;
import com.orders.domain.OrderRevenueDTO;
import com.streams.management.order.service.OrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RequestMapping("/v1/orders")
@RestController
public class OrdersController {

    private OrderService orderService;

    public OrdersController(OrderService orderService) {
        this.orderService = orderService;
    }


    @GetMapping("/count/{order_type}")
    public ResponseEntity<?> ordersCount(@PathVariable("order_type") String orderType,
                                         @RequestParam(value = "location_id", required = false) String locationId) {
        if (StringUtils.hasLength(locationId)) {
            log.info("Retrieving orders count per store for orderType: {} and location_id : {}", orderType, locationId);
            OrderCountPerStoreDTO ordersCountByLocation = orderService.getOrdersCountByLocationId(orderType, locationId);
            log.info("Order counts returned are : {}", ordersCountByLocation);
            return new ResponseEntity<>(ordersCountByLocation, HttpStatus.OK);
        } else {
            log.info("Retrieving orders count per store for orderType: {}", orderType);
            List<OrderCountPerStoreDTO> ordersCount = orderService.getOrdersCount(orderType);
            log.info("Order per store returned are : {}", ordersCount);
            return new ResponseEntity<>(ordersCount, HttpStatus.OK);
        }
    }

    @GetMapping("/count")
    public ResponseEntity<List<AllOrdersCountPerStoreDTO>> allOrdersCountPerStore() {
        log.info("retrieving all orders count per store for all orderTypes....");
        List<AllOrdersCountPerStoreDTO> list = orderService.getAllOrdersCount();
        return new ResponseEntity<>(list, HttpStatus.OK);
    }

    @GetMapping("/revenue/{order_type}")
    public ResponseEntity<?> revenueByOrderType(@PathVariable("order_type") String orderType,
                                                @RequestParam(value = "location_id", required = false) String locationId) {
        if (StringUtils.hasLength(locationId)) {
            log.info("Retrieving revenue for order_type : {} and location-id", orderType, locationId);
            return new ResponseEntity<>(orderService.revenueByOrderTypeAndLocationId(orderType, locationId), HttpStatus.OK);
        } else {
            log.info("Retrieving revenue for order-type : {}", orderType);
            return new ResponseEntity<>(orderService.revenueByOrderType(orderType), HttpStatus.OK);
        }
    }
}
