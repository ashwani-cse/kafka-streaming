package com.streams.management.order.topology;

import com.orders.domain.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OrdersTopologyTest {

    TopologyTestDriver testDriver;
    TestInputTopic<String, Order> inputTopic;
    OrdersTopology ordersTopology = new OrdersTopology();
    StreamsBuilder streamsBuilder = null;


    @BeforeEach
    public void setup() {
        // testDriver = new TopologyTestDriver(OrdersTopology.buildTopology()); // this will not work because we are using springboot streambuilder so we have to create
        /*
         * We cannot directly instantiate the TopologyTestDriver with OrdersTopology.buildTopology() as we have created in non-springboot project
         * because we are using Spring Boot's StreamsBuilder. Instead, we need to create an instance of
         * OrdersTopology as shown above using new keyword, then create a StreamsBuilder instance as null.
         * The process method of OrdersTopology will be called to assign the actual topology to the StreamsBuilder.
         * After that, we can use the StreamsBuilder's build() method to create the topology,
         * which is then required by the TopologyTestDriver.
         */

        streamsBuilder = new StreamsBuilder();
        ordersTopology.process(streamsBuilder);

        Topology topology = streamsBuilder.build();
        testDriver = new TopologyTestDriver(topology);

        inputTopic = testDriver.createInputTopic(OrdersTopology.ORDERS,
                Serdes.String().serializer(), new JsonSerde<>(Order.class).serializer());
    }

    /*
     *  To teardown the test driver after each test
     * */
    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    // Add test cases here
    @Test
    void ordersCount() {
        //publish data to input topic
        inputTopic.pipeKeyValueList(getOrders());

        // read data from state store - general-orders
        KeyValueStore<String, Long> generalOrdersStore = testDriver.getKeyValueStore(OrdersTopology.GENERAL_ORDERS_COUNT);
        Long store1234_orderCounts = generalOrdersStore.get("store_1234");
        assertEquals(1, store1234_orderCounts);

        KeyValueStore<String, Long> restaurantOrdersStore = testDriver.getKeyValueStore(OrdersTopology.RESTAURANT_ORDERS_COUNT);
        Long store1234_restaurant_orderCounts = restaurantOrdersStore.get("store_1234");
        assertEquals(1, store1234_restaurant_orderCounts);

    }

    @Test
    void ordersRevenue() {
        //publish data to input topic
        inputTopic.pipeKeyValueList(getOrders());

        // read data from state store - general-orders
        KeyValueStore<String, TotalRevenue> generalOrdersStore = testDriver.getKeyValueStore(OrdersTopology.GENERAL_ORDERS_REVENUE);
        TotalRevenue store1234_orderCounts = generalOrdersStore.get("store_1234");
        assertEquals(new BigDecimal("27.00"), store1234_orderCounts.runningRevenue());

        KeyValueStore<String, TotalRevenue> restaurantOrdersStore = testDriver.getKeyValueStore(OrdersTopology.RESTAURANT_ORDERS_REVENUE);
        TotalRevenue store1234_restaurant_orderCounts = restaurantOrdersStore.get("store_1234");
        assertEquals(new BigDecimal("15.00"), store1234_restaurant_orderCounts.runningRevenue());

    }

    @Test
    void ordersRevenue_multiple() {
        inputTopic.pipeKeyValueList(getOrders());
        inputTopic.pipeKeyValueList(getOrders());

        KeyValueStore<String, TotalRevenue> generalOrdersStore = testDriver.getKeyValueStore(OrdersTopology.GENERAL_ORDERS_REVENUE);
        TotalRevenue store1234_orderCounts = generalOrdersStore.get("store_1234");
        assertEquals(new BigDecimal("54.00"), store1234_orderCounts.runningRevenue());

        KeyValueStore<String, TotalRevenue> restaurantOrdersStore = testDriver.getKeyValueStore(OrdersTopology.RESTAURANT_ORDERS_REVENUE);
        TotalRevenue store1234_restaurant_orderCounts = restaurantOrdersStore.get("store_1234");
        assertEquals(new BigDecimal("30.00"), store1234_restaurant_orderCounts.runningRevenue());

    }

    // mock data
    static List<KeyValue<String, Order>> getOrders() {

        var orderItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var orderItemsRestaurant = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new Order(12345, "store_1234",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.now()
        );

        var order2 = new Order(54321, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                LocalDateTime.now()
        );

        var order3 = new Order(12345, "store_4567",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.now()
        );

        var order4 = new Order(12345, "store_4567",
                new BigDecimal("27.00"),
                OrderType.RESTAURANT,
                orderItems,
                LocalDateTime.now()
        );

        KeyValue<String, Order> kv1 = KeyValue.pair(order1.orderId().toString(), order1);
        KeyValue<String, Order> kv2 = KeyValue.pair(order2.orderId().toString(), order2);
        KeyValue<String, Order> kv3 = KeyValue.pair(order3.orderId().toString(), order3);
        KeyValue<String, Order> kv4 = KeyValue.pair(order4.orderId().toString(), order4);

        return List.of(kv1, kv2, kv3, kv4);
    }
}