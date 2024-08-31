package com.streams.management.order.service;

import com.orders.domain.OrderCountPerStoreByWindowsDTO;
import com.orders.domain.OrderType;
import com.orders.domain.OrdersRevenuePerStoreByWindowsDTOP;
import com.orders.domain.TotalRevenue;
import com.streams.management.order.util.CommonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.streams.management.order.topology.OrdersTopology.*;

@Slf4j
@Service
public class OrderWindowService {

    private OrderStoreService ordersStoreService;

    public OrderWindowService(OrderStoreService ordersStoreService) {
        this.ordersStoreService = ordersStoreService;
    }

    private ReadOnlyWindowStore<String, Long> getCountWindowsStateStore(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> ordersStoreService.ordersWindowsCountStore(GENERAL_ORDERS_COUNT_WINDOWS);
            case RESTAURANT_ORDERS -> ordersStoreService.ordersWindowsCountStore(RESTAURANT_ORDERS_COUNT_WINDOWS);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    private ReadOnlyWindowStore<String, TotalRevenue> getRevenueWindowsStateStore(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> ordersStoreService.ordersWindowsRevenueStore(GENERAL_ORDERS_REVENUE_WINDOWS);
            case RESTAURANT_ORDERS -> ordersStoreService.ordersWindowsRevenueStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    private List<OrderCountPerStoreByWindowsDTO> mapToOrderCountPerStoreByWindowsDTO(KeyValueIterator<Windowed<String>, Long> windowedKeyValueIterator, OrderType orderType) {
        Spliterator<KeyValue<Windowed<String>, Long>> spliterator = Spliterators.spliteratorUnknownSize(windowedKeyValueIterator, 0);

        Stream<KeyValue<Windowed<String>, Long>> stream = StreamSupport.stream(spliterator, false);

        return stream.map(windowedKVPair -> new OrderCountPerStoreByWindowsDTO(
                        windowedKVPair.key.key(),
                        windowedKVPair.value.longValue(),
                        orderType,
                        LocalDateTime.ofInstant(windowedKVPair.key.window().startTime(), ZoneId.of("GMT")),
                        LocalDateTime.ofInstant(windowedKVPair.key.window().endTime(), ZoneId.of("GMT"))
                )
        ).collect(Collectors.toList());
    }


    public List<OrderCountPerStoreByWindowsDTO> getOrdersCountWindowsByType(String orderType) {

        ReadOnlyWindowStore<String, Long> countWindowsStateStore = getCountWindowsStateStore(orderType);

        KeyValueIterator<Windowed<String>, Long> windowedKeyValueIterator = countWindowsStateStore.all();

        return mapToOrderCountPerStoreByWindowsDTO(windowedKeyValueIterator, CommonUtil.mapOrderType(orderType));
    }

    public OrderCountPerStoreByWindowsDTO getOrdersCountWindowsByLocationId(String orderType, String locationId) {
        return null;
    }

    public List<OrderCountPerStoreByWindowsDTO> getAllOrdersCountByWindows() {
        List<OrderCountPerStoreByWindowsDTO> generalOrdersByWindow = getOrdersCountWindowsByType(GENERAL_ORDERS);
        List<OrderCountPerStoreByWindowsDTO> restaurantOrdersByWindow = getOrdersCountWindowsByType(RESTAURANT_ORDERS);
        return Stream.of(generalOrdersByWindow, restaurantOrdersByWindow)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<OrderCountPerStoreByWindowsDTO> getAllOrdersCountByWindows(LocalDateTime fromTime, LocalDateTime toTime) {
        Instant fromTimeInstant = fromTime.toInstant(ZoneOffset.UTC);
        Instant toTimeInstant = toTime.toInstant(ZoneOffset.UTC);
        KeyValueIterator<Windowed<String>, Long> generalOrdersKVIterator =
                getCountWindowsStateStore(GENERAL_ORDERS)
                        //.backwardFetchAll(fromTimeInstant, toTimeInstant); // reverse time window
                        .fetchAll(fromTimeInstant, toTimeInstant);
        KeyValueIterator<Windowed<String>, Long> restaurantOrdersKVIterator =
                getCountWindowsStateStore(RESTAURANT_ORDERS)
                        //.backwardFetchAll(fromTimeInstant, toTimeInstant);
                        .fetchAll(fromTimeInstant, toTimeInstant);

        List<OrderCountPerStoreByWindowsDTO> generalOrdersCounts = mapToOrderCountPerStoreByWindowsDTO(generalOrdersKVIterator, OrderType.GENERAL);
        List<OrderCountPerStoreByWindowsDTO> restaurantOrdersCounts = mapToOrderCountPerStoreByWindowsDTO(restaurantOrdersKVIterator, OrderType.GENERAL);
        return Stream.of(generalOrdersCounts, restaurantOrdersCounts)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<OrdersRevenuePerStoreByWindowsDTOP> getOrdersRevenueWindowsByType(String orderType) {
        ReadOnlyWindowStore<String, TotalRevenue> revenueWindowsStateStore = getRevenueWindowsStateStore(orderType);
        KeyValueIterator<Windowed<String>, TotalRevenue> windowedKeyValueIterator = revenueWindowsStateStore.all();
        return mapToOrderRevenuePerStoreByWindowsDTO(windowedKeyValueIterator, CommonUtil.mapOrderType(orderType));
    }

    public OrdersRevenuePerStoreByWindowsDTOP getOrderRrevenueWindowsByLocationId(String orderType, String locationId) {
        return null;
    }

    private List<OrdersRevenuePerStoreByWindowsDTOP> mapToOrderRevenuePerStoreByWindowsDTO(KeyValueIterator<Windowed<String>, TotalRevenue> windowedKeyValueIterator, OrderType orderType) {
        Spliterator<KeyValue<Windowed<String>, TotalRevenue>> spliterator = Spliterators.spliteratorUnknownSize(windowedKeyValueIterator, 0);

        Stream<KeyValue<Windowed<String>, TotalRevenue>> stream = StreamSupport.stream(spliterator, false);

        return stream.map(windowedKVPair -> new OrdersRevenuePerStoreByWindowsDTOP(
                        windowedKVPair.key.key(),
                        windowedKVPair.value,
                        orderType,
                        LocalDateTime.ofInstant(windowedKVPair.key.window().startTime(), ZoneId.of("GMT")),
                        LocalDateTime.ofInstant(windowedKVPair.key.window().endTime(), ZoneId.of("GMT"))
                )
        ).collect(Collectors.toList());
    }
}
