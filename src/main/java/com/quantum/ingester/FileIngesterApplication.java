package com.quantum.ingester;

import com.quantum.common.util.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;

@SpringBootApplication
public class FileIngesterApplication {

    private static final Logger log = LoggerFactory.getLogger(FileIngesterApplication.class);

    @Value("${spring.application.name:quantum-file-ingester}")
    private String appName;

    @Value("${spring.profiles.active:local}")
    private String activeProfile;

    public static void main(String[] args) {
        SpringApplication.run(FileIngesterApplication.class, args);
    }

    @PostConstruct
    public void init() {
        // R3: Initialize service context for structured logging
        LoggingUtils.initServiceContext(appName, "2.0.0-SNAPSHOT", activeProfile);
        // R21: Audit service start
        LoggingUtils.audit("SERVICE_START", appName, "SUCCESS", "File Ingester started");
        log.info("Quantum File Ingester started (profile: {})", activeProfile);
    }
}
