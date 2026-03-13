package com.quantum.ingester.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quantum.ingester.engine.RulesEngine;
import com.quantum.ingester.model.RendicionContext;
import com.quantum.ingester.schema.IngestionSchema;
import com.quantum.ingester.schema.SchemaLoader;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Processor de Camel que ejecuta el pipeline completo de procesamiento
 * sobre un archivo JSONL antes del split/chunk dispatch a JMS.
 *
 * <p>
 * Pipeline por linea:
 * <ol>
 * <li>Parsea la linea como JsonNode</li>
 * <li>Valida contra el schema de la institucion (ValidationDelegate →
 * RulesEngine)</li>
 * <li>Acumula sumas de contribuciones (CalculationDelegate → RulesEngine)</li>
 * <li>Acumula en lote para insercion MySQL (DbInsertDelegate →
 * RulesEngine)</li>
 * </ol>
 * </p>
 *
 * <p>
 * Al terminar el archivo, guarda el {@link RendicionContext} en el property
 * {@code rendicionContext} del exchange para que
 * {@link com.quantum.ingester.generator.RendicionGenerator}
 * lo use al generar el .ren.
 * </p>
 *
 * <p>
 * Lee el archivo directamente desde el path {@code CamelFileAbsolutePath} para
 * no interferir con el body del exchange (que el route sigue usando para el
 * split JMS).
 * </p>
 */
@Component
public class RecordPipelineProcessor implements Processor {

    private static final Logger log = LoggerFactory.getLogger(RecordPipelineProcessor.class);

    /** Cantidad de registros por lote de insercion MySQL */
    private static final int DB_BATCH_SIZE = 500;

    private final SchemaLoader schemaLoader;
    private final RulesEngine rulesEngine;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbc;

    public RecordPipelineProcessor(SchemaLoader schemaLoader,
            RulesEngine rulesEngine,
            ObjectMapper objectMapper,
            JdbcTemplate jdbc) {
        this.schemaLoader = schemaLoader;
        this.rulesEngine = rulesEngine;
        this.objectMapper = objectMapper;
        this.jdbc = jdbc;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String filePath = exchange.getIn().getHeader("CamelFileAbsolutePath", String.class);
        String fileName = exchange.getIn().getHeader("CamelFileName", String.class);

        log.info("[RecordPipeline] Iniciando pipeline para: {}", fileName);
        long t0 = System.currentTimeMillis();

        // Determinar institucion del primer registro del archivo
        String institution = detectInstitution(filePath);
        String period = detectPeriod(filePath);

        IngestionSchema schema = schemaLoader.getSchema(institution);
        if (schema == null) {
            log.warn("[RecordPipeline] Sin schema para institucion '{}', usando validacion basica", institution);
        }

        RendicionContext context = new RendicionContext(fileName, institution, period);
        List<JsonNode> batch = new ArrayList<>(DB_BATCH_SIZE);

        // Streaming line-by-line sobre el archivo JSONL
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.strip();
                if (line.isEmpty())
                    continue;

                context.incrementRead();

                JsonNode record;
                try {
                    record = objectMapper.readTree(line);
                } catch (Exception e) {
                    context.incrementError();
                    context.addError("JSON parse error en linea " + context.getRecordsRead() + ": " + e.getMessage());
                    continue;
                }

                // ── ValidationDelegate ───────────────────────────────────────
                List<String> errors = rulesEngine.validate(record, schema);
                if (!errors.isEmpty()) {
                    context.incrementError();
                    context.addError("Linea " + context.getRecordsRead() + ": " + errors.get(0));
                    continue;
                }
                context.incrementValid();

                // ── CalculationDelegate ──────────────────────────────────────
                rulesEngine.applyCalculations(record, schema, context);

                // ── DbInsertDelegate (batch) ─────────────────────────────────
                batch.add(record);
                if (batch.size() >= DB_BATCH_SIZE) {
                    rulesEngine.batchInsert(batch, schema, context);
                    batch.clear();
                }
            }

            // Flush ultimo lote
            if (!batch.isEmpty()) {
                rulesEngine.batchInsert(batch, schema, context);
                batch.clear();
            }

        } catch (IOException e) {
            log.error("[RecordPipeline] Error leyendo archivo {}: {}", fileName, e.getMessage());
            context.addError("IO error: " + e.getMessage());
        }

        // ── Guardar resumen en base de datos ─────────────────────────────────
        persistRendicion(context);

        long elapsed = System.currentTimeMillis() - t0;
        log.info("[RecordPipeline] Completado {} — read={}, valid={}, error={}, inserted={}, dup={} en {}ms",
                fileName, context.getRecordsRead(), context.getRecordsValid(),
                context.getRecordsError(), context.getRecordsInserted(),
                context.getRecordsDuplicate(), elapsed);

        // ── Exponer contexto para RendicionGenerator ─────────────────────────
        exchange.setProperty("rendicionContext", context);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Lee la primera linea del JSONL para extraer la institucion de
     * _meta.institution.
     */
    private String detectInstitution(String filePath) {
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.strip();
                if (line.isEmpty())
                    continue;
                JsonNode root = objectMapper.readTree(line);
                String inst = root.path("_meta").path("institution").asText(null);
                if (inst != null && !inst.isBlank())
                    return inst.toUpperCase();
                break;
            }
        } catch (Exception e) {
            log.debug("detectInstitution failed: {}", e.getMessage());
        }
        return "UNKNOWN";
    }

    /**
     * Lee la primera linea del JSONL para extraer el periodo de _meta.period.
     */
    private String detectPeriod(String filePath) {
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.strip();
                if (line.isEmpty())
                    continue;
                JsonNode root = objectMapper.readTree(line);
                String period = root.path("_meta").path("period").asText(null);
                if (period != null && !period.isBlank())
                    return period;
                break;
            }
        } catch (Exception e) {
            log.debug("detectPeriod failed: {}", e.getMessage());
        }
        return "UNKNOWN";
    }

    /** Guarda el resumen del archivo en la tabla rendicion_resumen */
    private void persistRendicion(RendicionContext ctx) {
        try {
            String errors = ctx.getErrorSample().isEmpty() ? null
                    : String.join(" | ", ctx.getErrorSample());
            if (errors != null && errors.length() > 2000) {
                errors = errors.substring(0, 2000);
            }
            jdbc.update("""
                    INSERT INTO rendicion_resumen
                      (source_file, institution, period,
                       records_read, records_valid, records_error,
                       records_inserted, records_duplicate,
                       processing_time_ms, result, error_sample)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    ctx.getSourceFile(), ctx.getInstitution(), ctx.getPeriod(),
                    ctx.getRecordsRead(), ctx.getRecordsValid(), ctx.getRecordsError(),
                    ctx.getRecordsInserted(), ctx.getRecordsDuplicate(),
                    ctx.getProcessingTimeMs(), ctx.getResult(), errors);
        } catch (Exception e) {
            log.warn("No se pudo persistir rendicion_resumen para {}: {}", ctx.getSourceFile(), e.getMessage());
        }
    }
}
