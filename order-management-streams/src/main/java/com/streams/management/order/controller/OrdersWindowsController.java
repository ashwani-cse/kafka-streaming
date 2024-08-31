package com.streams.management.order.controller;

import com.orders.domain.OrderCountPerStoreByWindowsDTO;
import com.orders.domain.OrdersRevenuePerStoreByWindowsDTOP;
import com.streams.management.order.service.OrderWindowService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@RequestMapping("/v1/orders/windows")
@RestController
public class OrdersWindowsController {

    private OrderWindowService orderWindowService;

    public OrdersWindowsController(OrderWindowService orderWindowService) {
        this.orderWindowService = orderWindowService;
    }

    @GetMapping("/count/{order_type}")
    public ResponseEntity<?> ordersCount(@PathVariable("order_type") String orderType,
                                         @RequestParam(value = "location_id", required = false) String locationId) {
        if (StringUtils.hasLength(locationId)) {
            log.info("Windows: Retrieving orders count per store for orderType: {} and location_id : {}", orderType, locationId);
            OrderCountPerStoreByWindowsDTO ordersCountByLocation = orderWindowService.getOrdersCountWindowsByLocationId(orderType, locationId);
            log.info("Windows:Order counts returned are : {}", ordersCountByLocation);
            return new ResponseEntity<>(ordersCountByLocation, HttpStatus.OK);
        } else {
            log.info("Windows:Retrieving orders count per store for orderType: {}", orderType);
            List<OrderCountPerStoreByWindowsDTO> ordersCount = orderWindowService.getOrdersCountWindowsByType(orderType);
            log.info("Windows:Order per store returned are : {}", ordersCount);
            return new ResponseEntity<>(ordersCount, HttpStatus.OK);
        }
    }


    @GetMapping("/count")
    public ResponseEntity<List<OrderCountPerStoreByWindowsDTO>> getAllOrdersCountByWindows(
            @RequestParam(value = "from_time", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            LocalDateTime fromTime,
            @RequestParam(value = "to_time", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            LocalDateTime toTime) {
        List<OrderCountPerStoreByWindowsDTO> list = null;
        if (fromTime != null && toTime != null) {
            list = orderWindowService.getAllOrdersCountByWindows(fromTime, toTime);
            return new ResponseEntity<>(list, HttpStatus.OK);
        }
        list = orderWindowService.getAllOrdersCountByWindows();
        return new ResponseEntity<>(list, HttpStatus.OK);
    }

    @GetMapping("/revenue/{order_type}")
    public ResponseEntity<?> ordersRevenue(@PathVariable("order_type") String orderType,
                                         @RequestParam(value = "location_id", required = false) String locationId) {
        if (StringUtils.hasLength(locationId)) {
            log.info("Windows: Retrieving orders revenue per store for orderType: {} and location_id : {}", orderType, locationId);
            OrdersRevenuePerStoreByWindowsDTOP ordersCountByLocation = orderWindowService.getOrderRrevenueWindowsByLocationId(orderType, locationId);
            log.info("Windows:Order revenue returned are : {}", ordersCountByLocation);
            return new ResponseEntity<>(ordersCountByLocation, HttpStatus.OK);
        } else {
            log.info("Windows:Retrieving orders revenue per store for orderType: {}", orderType);
            List<OrdersRevenuePerStoreByWindowsDTOP> ordersCount = orderWindowService.getOrdersRevenueWindowsByType(orderType);
            log.info("Windows:Order per store returned are : {}", ordersCount);
            return new ResponseEntity<>(ordersCount, HttpStatus.OK);
        }
    }

}
