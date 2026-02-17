package com.quantum.ingester.route;

import com.quantum.common.config.QueueConstants;
import com.quantum.common.model.Chunk;
import com.quantum.ingester.processor.ChunkMetadataProcessor;
import com.quantum.ingester.processor.FileValidationProcessor;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.stereotype.Component;

@Component
public class FileIngestionRoute extends RouteBuilder {

        private final ChunkMetadataProcessor chunkMetadataProcessor;
        private final FileValidationProcessor fileValidationProcessor;

        public FileIngestionRoute(ChunkMetadataProcessor chunkMetadataProcessor,
                        FileValidationProcessor fileValidationProcessor) {
                this.chunkMetadataProcessor = chunkMetadataProcessor;
                this.fileValidationProcessor = fileValidationProcessor;
        }

        @Override
        public void configure() throws Exception {

                JacksonDataFormat jacksonDataFormat = new JacksonDataFormat(Chunk.class);
                jacksonDataFormat.addModule(new JavaTimeModule());
                jacksonDataFormat.disableFeature(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

                onException(Exception.class)
                                .maximumRedeliveries(QueueConstants.MAX_REDELIVERY_ATTEMPTS)
                                .redeliveryDelay(QueueConstants.REDELIVERY_DELAY_MS)
                                .retryAttemptedLogLevel(LoggingLevel.WARN)
                                .handled(true)
                                .log(LoggingLevel.ERROR, "Failed to process file: ${exception.message}")
                                .to("direct:error-handler");

                from("file:{{ingester.input-dir}}?noop={{ingester.noop}}&readLock=changed&readLockTimeout=30000")
                                .routeId("file-ingestion-route")
                                .log(LoggingLevel.INFO, "Started processing file: ${header.CamelFileName}")
                                .process(fileValidationProcessor)
                                .split(body().tokenize("\n", QueueConstants.DEFAULT_CHUNK_SIZE, false))
                                .streaming()
                                .stopOnException()
                                .parallelProcessing(false)
                                .process(chunkMetadataProcessor)
                                .marshal(jacksonDataFormat)
                                .convertBodyTo(String.class)
                                .to("jms:queue:" + QueueConstants.CHUNK_QUEUE
                                                + "?deliveryPersistent=true"
                                                + "&explicitQosEnabled=true"
                                                + "&deliveryMode=2")
                                .log(LoggingLevel.DEBUG,
                                                "Sent chunk ${header.chunkIndex} of file ${header.CamelFileName}")
                                .end()
                                .process(exchange -> {
                                        String fileName = exchange.getIn().getHeader("CamelFileName", String.class);
                                        chunkMetadataProcessor.cleanupCounter(fileName);
                                })
                                .log(LoggingLevel.INFO,
                                                "Completed splitting file: ${header.CamelFileName} into ${header.totalChunks} chunks");

                from("direct:error-handler")
                                .routeId("error-handler-route")
                                .log(LoggingLevel.ERROR, "Error handler invoked for file: ${header.CamelFileName}")
                                .to("jms:queue:" + QueueConstants.DLQ);
        }
}
