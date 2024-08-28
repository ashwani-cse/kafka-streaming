package com.kafka.streams.app.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.streams.app.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GreetingStreamsTopology {

    private ObjectMapper objectMapper;

    public static String GREETINGS = "greetings";
    public static String GREETINGS_OUTPUT = "greetings-output";

    public GreetingStreamsTopology(ObjectMapper objectMapper){
        this.objectMapper = objectMapper;
    }

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        //KStream<String, String> greetingStream = streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, Greeting> greetingStream = streamsBuilder
                .stream(GREETINGS,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Greeting.class, objectMapper)));

        greetingStream
                .print(Printed.<String, Greeting>toSysOut().withLabel("greetings-stream"));

        KStream<String, Greeting> modifiedStream = greetingStream
                .mapValues((readOnlyKey, value) ->
                        //value.toUpperCase())
                        new Greeting(value.message().toUpperCase(), value.localDateTime())
                );

        modifiedStream
                .print(Printed.<String, Greeting>toSysOut().withLabel("modified-stream"));

        modifiedStream.to(GREETINGS_OUTPUT,
                Produced.with(Serdes.String(), new JsonSerde<>(Greeting.class, objectMapper)));


    }


}
