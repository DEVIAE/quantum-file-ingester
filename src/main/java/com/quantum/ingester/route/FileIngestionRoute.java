package com.quantum.ingester.route;

import com.quantum.common.config.QueueConstants;
import com.quantum.ingester.processor.ChunkMetadataProcessor;
import com.quantum.ingester.processor.FileValidationProcessor;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
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

        onException(Exception.class)
                .maximumRedeliveries(QueueConstants.MAX_REDELIVERY_ATTEMPTS)
                .redeliveryDelay(QueueConstants.REDELIVERY_DELAY_MS)
                .retryAttemptedLogLevel(LoggingLevel.WARN)
                .handled(true)
                .log(LoggingLevel.ERROR, "Failed to process file: ${exception.message}")
                .to("direct:error-handler");

        from("file:{{ingester.input-dir}}?noop={{ingester.noop}}&streamDownload=true&readLock=changed&readLockTimeout=30000")
                .routeId("file-ingestion-route")
                .log(LoggingLevel.INFO, "Started processing file: ${header.CamelFileName}")
                .process(fileValidationProcessor)
                .split(body().tokenize("\n", QueueConstants.DEFAULT_CHUNK_SIZE, false))
                    .streaming()
                    .parallelProcessing(false)
                    .process(chunkMetadataProcessor)
                    .marshal().json()
                    .to("jms:queue:" + QueueConstants.CHUNK_QUEUE
                            + "?deliveryPersistent=true"
                            + "&explicitQosEnabled=true"
                            + "&deliveryMode=2")
                    .log(LoggingLevel.DEBUG, "Sent chunk ${header.chunkIndex} of file ${header.CamelFileName}")
                .end()
                .log(LoggingLevel.INFO, "Completed splitting file: ${header.CamelFileName} into ${header.totalChunks} chunks");

        from("direct:error-handler")
                .routeId("error-handler-route")
                .log(LoggingLevel.ERROR, "Error handler invoked for file: ${header.CamelFileName}")
                .to("jms:queue:" + QueueConstants.DLQ);
    }
}
