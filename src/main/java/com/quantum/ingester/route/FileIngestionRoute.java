package com.quantum.ingester.route;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.quantum.common.config.QueueConstants;
import com.quantum.common.model.Chunk;
import com.quantum.ingester.processor.ChunkMetadataProcessor;
import com.quantum.ingester.processor.FileValidationProcessor;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Ruta Camel de ingesta del archivo.
 *
 * <h2>Flujo</h2>
 * <pre>
 *   /data/process/   (sftp-gateway deposita archivos estables aquí)
 *      │  readLock=changed — espera que el archivo deje de crecer
 *      ↓  [FileValidationProcessor] — valida header/formato
 *      ↓  split por líneas en chunks → JMS: chunks
 *      ↓  on success: move → /data/processed/<filename>
 *      ↓  on failure: move → /data/failed/<filename>
 *                  + escribe /data/output/<filename>.error.json
 * </pre>
 */
@Component
public class FileIngestionRoute extends RouteBuilder {

    private final ChunkMetadataProcessor chunkMetadataProcessor;
    private final FileValidationProcessor fileValidationProcessor;
    private final ObjectMapper objectMapper;

    @Value("${ingester.input-dir:/data/process}")
    private String inputDir;

    @Value("${ingester.processed-dir:/data/processed}")
    private String processedDir;

    @Value("${ingester.failed-dir:/data/failed}")
    private String failedDir;

    @Value("${ingester.output-dir:/data/output}")
    private String outputDir;

    public FileIngestionRoute(ChunkMetadataProcessor chunkMetadataProcessor,
                              FileValidationProcessor fileValidationProcessor) {
        this.chunkMetadataProcessor = chunkMetadataProcessor;
        this.fileValidationProcessor = fileValidationProcessor;
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void configure() throws Exception {

        JacksonDataFormat jacksonDataFormat = new JacksonDataFormat(Chunk.class);
        jacksonDataFormat.addModule(new JavaTimeModule());
        jacksonDataFormat.disableFeature(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // ── Manejo global de errores ──────────────────────────────────────────
        // El archivo ya fue movido a /data/failed/ por moveFailed del file: component.
        // Aquí generamos el status file de error en /data/output/.
        onException(Exception.class)
                .maximumRedeliveries(QueueConstants.MAX_REDELIVERY_ATTEMPTS)
                .redeliveryDelay(QueueConstants.REDELIVERY_DELAY_MS)
                .retryAttemptedLogLevel(LoggingLevel.WARN)
                .handled(true)
                .log(LoggingLevel.ERROR, "Failed to ingest file '${header.CamelFileName}': ${exception.message}")
                .process(exchange -> {
                    String fileName   = exchange.getIn().getHeader("CamelFileName", String.class);
                    String errorMsg   = exchange.getProperty("CamelExceptionCaught", Exception.class) != null
                            ? exchange.getProperty("CamelExceptionCaught", Exception.class).getMessage()
                            : "Unknown error";
                    writeStatusFile(outputDir, fileName, "FAILED", errorMsg, 0, 0);
                })
                .to("jms:queue:" + QueueConstants.DLQ);

        // ── Ruta principal ────────────────────────────────────────────────────
        // move        → archive del archivo original tras éxito
        // moveFailed  → mover a /data/failed/ en caso de error (Camel lo maneja antes del onException)
        from("file:" + inputDir
                + "?readLock=changed"
                + "&readLockTimeout=30000"
                + "&recursive=false"
                + "&move=" + processedDir + "/${file:name}"
                + "&moveFailed=" + failedDir + "/${file:name}")
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
                    .log(LoggingLevel.DEBUG, "Sent chunk ${header.chunkIndex} of ${header.CamelFileName}")
                .end()
                .process(exchange -> {
                    String fileName    = exchange.getIn().getHeader("CamelFileName", String.class);
                    int    totalChunks = exchange.getIn().getHeader("totalChunks", 0, Integer.class);
                    chunkMetadataProcessor.cleanupCounter(fileName);
                    // Escribir status de ingesta exitosa en /data/output/
                    writeStatusFile(outputDir, fileName, "INGESTED", null, totalChunks, 0);
                })
                .log(LoggingLevel.INFO,
                        "Completed ingesting file: ${header.CamelFileName} into ${header.totalChunks} chunks"
                        + " → moved to " + processedDir);
    }

    /** Escribe un archivo JSON de estado en el directorio de salida. */
    private void writeStatusFile(String dir, String fileName, String status,
                                  String errorMessage, int totalChunks, int failedChunks) {
        try {
            Files.createDirectories(Paths.get(dir));
            String baseName  = fileName != null ? fileName : "unknown";
            String statusExt = "FAILED".equals(status) ? ".error.json" : ".ingested.json";
            Path   outFile   = Paths.get(dir, baseName + statusExt);

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("fileName",     baseName);
            payload.put("status",       status);
            payload.put("timestamp",    Instant.now().toString());
            payload.put("service",      "quantum-file-ingester");
            payload.put("totalChunks",  totalChunks);
            payload.put("failedChunks", failedChunks);
            if (errorMessage != null) {
                payload.put("error", errorMessage);
                payload.put("failedPath", failedDir + File.separator + baseName);
            }

            Files.write(outFile, objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(payload).getBytes(StandardCharsets.UTF_8));

        } catch (Exception ex) {
            log.error("Could not write status file for '{}': {}", fileName, ex.getMessage());
        }
    }
}

