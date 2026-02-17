package com.quantum.ingester.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.file.GenericFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
public class FileValidationProcessor implements Processor {

    private static final Logger log = LoggerFactory.getLogger(FileValidationProcessor.class);

    private static final long MAX_FILE_SIZE = 2L * 1024 * 1024 * 1024;

    @Override
    public void process(Exchange exchange) throws Exception {
        String fileName = exchange.getIn().getHeader("CamelFileName", String.class);

        // Use header for file size — do NOT read the body (avoids loading entire file into memory)
        Long fileSize = exchange.getIn().getHeader("CamelFileLength", Long.class);

        if (fileSize == null) {
            // Fallback: get size from GenericFile without reading content
            Object body = exchange.getIn().getBody();
            if (body instanceof GenericFile<?> genericFile) {
                File file = (File) genericFile.getFile();
                fileSize = file.length();
            }
        }

        if (fileName == null || fileName.isBlank()) {
            throw new IllegalArgumentException("File name is required");
        }

        if (fileSize != null && fileSize > MAX_FILE_SIZE) {
            throw new IllegalArgumentException(
                    "File size " + fileSize + " exceeds maximum allowed size of " + MAX_FILE_SIZE);
        }

        // Store fileSize in header for downstream processors
        exchange.getIn().setHeader("fileSize", fileSize);
        log.info("Validated file: {} (size: {} bytes)", fileName, fileSize);
    }
}
