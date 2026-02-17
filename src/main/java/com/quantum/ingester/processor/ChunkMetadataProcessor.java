package com.quantum.ingester.processor;

import com.quantum.common.model.Chunk;
import com.quantum.common.util.ChunkIdGenerator;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ChunkMetadataProcessor implements Processor {

    private static final Logger log = LoggerFactory.getLogger(ChunkMetadataProcessor.class);

    private final ConcurrentHashMap<String, AtomicInteger> fileCounters = new ConcurrentHashMap<>();

    @Override
    public void process(Exchange exchange) throws Exception {
        String body = exchange.getIn().getBody(String.class);
        String fileName = exchange.getIn().getHeader("CamelFileName", String.class);

        AtomicInteger counter = fileCounters.computeIfAbsent(fileName, k -> new AtomicInteger(0));
        int chunkIndex = counter.getAndIncrement();

        // CamelSplitSize = total number of split parts (available from 2nd iteration onward)
        Integer splitSize = exchange.getProperty("CamelSplitSize", Integer.class);
        int totalChunks = splitSize != null ? splitSize : -1;

        // Parse lines efficiently — avoid creating intermediate String array
        List<String> lines = parseLines(body);
        String chunkId = ChunkIdGenerator.generate(fileName, chunkIndex);

        Chunk chunk = new Chunk(
                chunkId,
                chunkIndex,
                totalChunks,
                fileName,
                Instant.now(),
                lines);

        // Release body reference early to help GC
        exchange.getIn().setBody(chunk);
        exchange.getIn().setHeader("chunkId", chunkId);
        exchange.getIn().setHeader("chunkIndex", chunkIndex);
        exchange.getIn().setHeader("totalChunks", totalChunks);
        exchange.getIn().setHeader("fileName", fileName);

        if (chunkIndex % 50 == 0) {
            log.info("Processing chunk {} of file {} ({} lines)", chunkIndex, fileName, lines.size());
        }
    }

    /**
     * Parse lines without creating an intermediate String[] array.
     * More memory-efficient than body.split("\n") for large chunks.
     */
    private List<String> parseLines(String body) {
        if (body == null || body.isEmpty()) {
            return List.of();
        }
        List<String> lines = new ArrayList<>();
        int start = 0;
        int idx;
        while ((idx = body.indexOf('\n', start)) != -1) {
            String line = body.substring(start, idx);
            if (!line.isEmpty()) {
                lines.add(line);
            }
            start = idx + 1;
        }
        // Last line (no trailing newline)
        if (start < body.length()) {
            lines.add(body.substring(start));
        }
        return lines;
    }

    /**
     * Clean up counters for a file after processing completes.
     * Called from route onCompletion.
     */
    public void cleanupCounter(String fileName) {
        fileCounters.remove(fileName);
    }

    public void resetCounter() {
        fileCounters.clear();
    }
}
