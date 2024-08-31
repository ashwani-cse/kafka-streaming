package com.streams.management.order.service;

import com.orders.domain.TotalRevenue;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
public class OrderStoreService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public OrderStoreService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public ReadOnlyKeyValueStore<String, Long> ordersCountStore(String storeName) {
        QueryableStoreType<ReadOnlyKeyValueStore<String, Long>> queryableStoreType = QueryableStoreTypes.keyValueStore();
        StoreQueryParameters<ReadOnlyKeyValueStore<String, Long>> queryParameters
                = StoreQueryParameters.fromNameAndType(storeName, queryableStoreType);
        return streamsBuilderFactoryBean
                .getKafkaStreams()
                .store(queryParameters);
    }

    public ReadOnlyKeyValueStore<String, TotalRevenue> totalRevenueStore(String storeName) {
        QueryableStoreType<ReadOnlyKeyValueStore<String, TotalRevenue>> queryableStoreType = QueryableStoreTypes.keyValueStore();
        StoreQueryParameters<ReadOnlyKeyValueStore<String, TotalRevenue>> queryParameters
                = StoreQueryParameters.fromNameAndType(storeName, queryableStoreType);
        return streamsBuilderFactoryBean
                .getKafkaStreams()
                .store(queryParameters);
    }

    public ReadOnlyWindowStore<String, Long> ordersWindowsCountStore(String storeName) {
        QueryableStoreType<ReadOnlyWindowStore<String, Long>> windowStateStore = QueryableStoreTypes.windowStore();
        StoreQueryParameters<ReadOnlyWindowStore<String, Long>> windowQueryParameters = StoreQueryParameters.fromNameAndType(storeName, windowStateStore);
        return streamsBuilderFactoryBean
                .getKafkaStreams()
                .store(windowQueryParameters);
    }

    // we can use generic code like below for all above function to avoid repeating code
    public ReadOnlyWindowStore<String, TotalRevenue> ordersWindowsRevenueStore(String storeName) {
        return getStore(storeName, QueryableStoreTypes.windowStore());
    }

    public <T> T getStore(String storeName, QueryableStoreType<T> stateStore) {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null) {
            throw new IllegalStateException("KafkaStreams is not initialized.");
        }

        StoreQueryParameters<T> queryParameters = StoreQueryParameters.fromNameAndType(storeName, stateStore);
        return kafkaStreams
                .store(StoreQueryParameters.fromNameAndType(
                        storeName,
                        stateStore)
                );
    }
}
