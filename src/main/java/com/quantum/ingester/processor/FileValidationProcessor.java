package com.quantum.ingester.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class FileValidationProcessor implements Processor {

    private static final Logger log = LoggerFactory.getLogger(FileValidationProcessor.class);

    private static final long MAX_FILE_SIZE = 2L * 1024 * 1024 * 1024;

    @Override
    public void process(Exchange exchange) throws Exception {
        String fileName = exchange.getIn().getHeader("CamelFileName", String.class);
        Long fileSize = exchange.getIn().getHeader("CamelFileLength", Long.class);

        if (fileName == null || fileName.isBlank()) {
            throw new IllegalArgumentException("File name is required");
        }

        if (fileSize != null && fileSize > MAX_FILE_SIZE) {
            throw new IllegalArgumentException(
                    "File size " + fileSize + " exceeds maximum allowed size of " + MAX_FILE_SIZE);
        }

        log.info("Validated file: {} (size: {} bytes)", fileName, fileSize);
    }
}
