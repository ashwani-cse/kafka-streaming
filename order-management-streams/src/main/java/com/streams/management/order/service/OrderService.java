package com.streams.management.order.service;

import com.orders.domain.*;
import com.streams.management.order.util.CommonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.streams.management.order.topology.OrdersTopology.*;

@Service
@Slf4j
public class OrderService {

    private OrderStoreService ordersStoreService;

    public OrderService(OrderStoreService ordersStoreService) {
        this.ordersStoreService = ordersStoreService;
    }


    public List<OrderCountPerStoreDTO> getOrdersCount(String orderType) {
        ReadOnlyKeyValueStore<String, Long> orderCountStore = getOrderStateStore(orderType);
        KeyValueIterator<String, Long> keyValueIterator = orderCountStore.all();
        Spliterator<KeyValue<String, Long>> keyValueSpliterator = Spliterators.spliteratorUnknownSize(keyValueIterator, 0);
        Stream<KeyValue<String, Long>> stream = StreamSupport.stream(keyValueSpliterator, false);
        List<OrderCountPerStoreDTO> countsPerStore = stream
                .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                .collect(Collectors.toList());

        return countsPerStore;
    }

    public OrderCountPerStoreDTO getOrdersCountByLocationId(String orderType, String locationId) {
        OrderCountPerStoreDTO orderCountPerStoreDTO = null;
        ReadOnlyKeyValueStore<String, Long> orderCountStore = getOrderStateStore(orderType);
        if (orderCountStore != null) {
            Long orderCount = orderCountStore.get(locationId);
            orderCountPerStoreDTO = new OrderCountPerStoreDTO(locationId, orderCount);
        }
        return orderCountPerStoreDTO;
    }

    public List<AllOrdersCountPerStoreDTO> getAllOrdersCount() {
        BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStoreDTO> mapper =
                (orderCountsPerStore, orderType) -> new AllOrdersCountPerStoreDTO(orderCountsPerStore.locationId(), orderCountsPerStore.orderCount(), orderType);

        List<AllOrdersCountPerStoreDTO> generalOrdersCount = getOrdersCount(GENERAL_ORDERS)
                .stream()
                .map(orderCountPerStore -> mapper.apply(orderCountPerStore, OrderType.GENERAL))
                .collect(Collectors.toList());

        List<AllOrdersCountPerStoreDTO> restaurantOrdersCount = getOrdersCount(RESTAURANT_ORDERS)
                .stream()
                .map(orderCountPerStore -> mapper.apply(orderCountPerStore, OrderType.RESTAURANT))
                .collect(Collectors.toList());

        return Stream.of(generalOrdersCount, restaurantOrdersCount)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<OrderRevenueDTO> revenueByOrderType(String orderType) {
        ReadOnlyKeyValueStore<String, TotalRevenue> revenueStateStore = getRevenueStateStore(orderType);
        KeyValueIterator<String, TotalRevenue> keyValueIterator = revenueStateStore.all();
        Spliterator<KeyValue<String, TotalRevenue>> keyValueSpliterator = Spliterators.spliteratorUnknownSize(keyValueIterator, 0);
        Stream<KeyValue<String, TotalRevenue>> stream = StreamSupport.stream(keyValueSpliterator, false);
        return stream.map(keyValue ->
                        new OrderRevenueDTO(keyValue.key,
                                CommonUtil.mapOrderType(orderType),
                                keyValue.value))
                .toList();
    }

    public OrderRevenueDTO revenueByOrderTypeAndLocationId(String orderType, String locationId) {
        OrderRevenueDTO orderRevenueDTO = null;
        ReadOnlyKeyValueStore<String, TotalRevenue> revenueStateStore = getRevenueStateStore(orderType);
        if(revenueStateStore!=null) {
            TotalRevenue totalRevenue = revenueStateStore.get(locationId);
            orderRevenueDTO = new OrderRevenueDTO(locationId, CommonUtil.mapOrderType(orderType), totalRevenue);
        }
        return orderRevenueDTO;
    }

    private ReadOnlyKeyValueStore<String, Long> getOrderStateStore(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> ordersStoreService.ordersCountStore(GENERAL_ORDERS_COUNT);
            case RESTAURANT_ORDERS -> ordersStoreService.ordersCountStore(RESTAURANT_ORDERS_COUNT);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    private ReadOnlyKeyValueStore<String, TotalRevenue> getRevenueStateStore(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> ordersStoreService.totalRevenueStore(GENERAL_ORDERS_REVENUE);
            case RESTAURANT_ORDERS -> ordersStoreService.totalRevenueStore(RESTAURANT_ORDERS_REVENUE);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }
}
