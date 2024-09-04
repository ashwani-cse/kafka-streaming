package com.streams.management.order.topology;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.orders.domain.Order;
import com.orders.domain.OrderCountPerStoreDTO;
import com.orders.domain.OrderLineItem;
import com.orders.domain.OrderType;
import com.streams.management.order.service.OrderService;
import org.apache.kafka.streams.KeyValue;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.streams.management.order.topology.OrdersTopology.ORDERS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;


@SpringBootTest
@EmbeddedKafka(topics = {ORDERS, OrdersTopology.STORES})
@TestPropertySource(properties = {
        "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
public class OrdersTopologyIntegrationTest {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    OrderService orderService;

    @Test
    void ordersCount(){
        //publish orders for test
        publishOrders();

      //  List<OrderCountPerStoreDTO> ordersCount = orderService.getOrdersCount(OrdersTopology.GENERAL_ORDERS);
      //  assert ordersCount.size() == 1;

        /*
        The above test will fail in give error:
        InvalidStateStoreException: Cannot get state store general_orders_count because the stream thread is STARTING, not RUNNING
    -:  The reason for this is that the whole kafka process is asynchronous so will have to wait until the process complete
        then query against state-store executes.
Solution: use library awaitility
        * */

        Awaitility.waitAtMost(10, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(()->
                        orderService.getOrdersCount(OrdersTopology.GENERAL_ORDERS).size(), notNullValue());
        List<OrderCountPerStoreDTO> ordersCount = orderService.getOrdersCount(OrdersTopology.GENERAL_ORDERS);
        System.out.println("################ list: "+ordersCount);
        assertEquals(2, ordersCount.size());

    }


    private void publishOrders() {
        orders()
                .forEach(order -> {
                    String orderJSON = null;
                    try {
                        orderJSON = objectMapper.writeValueAsString(order.value);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    kafkaTemplate.send(ORDERS, order.key, orderJSON);
                });


    }


    static List<KeyValue<String, Order>> orders() {

        var orderItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var orderItemsRestaurant = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new Order(77771, "store_1234",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                //LocalDateTime.now()
                LocalDateTime.parse("2023-02-21T21:25:01")
        );

        var order2 = new Order(88881, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                //LocalDateTime.now()
                LocalDateTime.parse("2023-02-21T21:25:01")
        );
        var keyValue1 = KeyValue.pair(order1.orderId().toString()
                , order1);

        var keyValue2 = KeyValue.pair(order2.orderId().toString()
                , order2);


        return List.of(keyValue1, keyValue2);

    }

}
