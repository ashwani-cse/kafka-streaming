package com.kafka.streams.app.exception;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

/**
 * @author Ashwani Kumar
 * Created on 18/08/24.
 */
@Slf4j
public class StreamProcessorCustomErrorHandler implements StreamsUncaughtExceptionHandler {
    /**
     * Inspect the exception received in a stream thread and respond with an action.
     *
     * @param exception the actual exception
     */
    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        log.error("Exception in stream processor is : {} " , exception.getMessage(), exception);
        if (exception instanceof StreamsException){
           // return StreamThreadExceptionResponse.REPLACE_THREAD; // Replace the thread, it is used when we want to replace the thread
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT; // Shutdown the client
        }
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    }
}