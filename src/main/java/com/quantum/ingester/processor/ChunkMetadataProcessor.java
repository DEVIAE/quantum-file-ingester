package com.quantum.ingester.processor;

import com.quantum.common.config.QueueConstants;
import com.quantum.common.model.Chunk;
import com.quantum.common.util.ChunkIdGenerator;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ChunkMetadataProcessor implements Processor {

    private final AtomicInteger chunkCounter = new AtomicInteger(0);

    @Override
    public void process(Exchange exchange) throws Exception {
        String body = exchange.getIn().getBody(String.class);
        String fileName = exchange.getIn().getHeader("CamelFileName", String.class);

        int chunkIndex = chunkCounter.getAndIncrement();

        Integer splitSize = exchange.getProperty("CamelSplitSize", Integer.class);
        int totalChunks = splitSize != null ? splitSize / QueueConstants.DEFAULT_CHUNK_SIZE + 1 : -1;

        List<String> lines = Arrays.asList(body.split("\n"));
        String chunkId = ChunkIdGenerator.generate(fileName, chunkIndex);

        Chunk chunk = new Chunk(
                chunkId,
                chunkIndex,
                totalChunks,
                fileName,
                Instant.now(),
                lines
        );

        exchange.getIn().setBody(chunk);
        exchange.getIn().setHeader("chunkId", chunkId);
        exchange.getIn().setHeader("chunkIndex", chunkIndex);
        exchange.getIn().setHeader("totalChunks", totalChunks);
        exchange.getIn().setHeader("fileName", fileName);
    }

    public void resetCounter() {
        chunkCounter.set(0);
    }
}
