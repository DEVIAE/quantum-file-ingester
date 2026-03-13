package com.quantum.ingester.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.quantum.ingester.model.RendicionContext;
import com.quantum.ingester.schema.IngestionSchema;
import com.quantum.ingester.schema.ValidationRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Motor central de reglas para el pipeline de ingestion.
 *
 * <p>
 * Tres responsabilidades:
 * <ol>
 * <li>{@link #validate} — valida un registro JSONL contra las reglas del
 * schema</li>
 * <li>{@link #applyCalculations} — extrae y acumula las sumas de
 * contribuciones</li>
 * <li>{@link #batchInsert} — inserta un lote de registros validados en
 * MySQL</li>
 * </ol>
 * </p>
 */
@Service
public class RulesEngine {

    private static final Logger log = LoggerFactory.getLogger(RulesEngine.class);

    private final JdbcTemplate jdbc;

    public RulesEngine(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // ── 1. Validacion ────────────────────────────────────────────────────────

    /**
     * Valida un nodo JSON canónico contra las reglas del schema.
     *
     * @param record nodo raiz del registro JSONL (contiene _meta, worker,
     *               contributions, …)
     * @param schema schema de la institucion
     * @return lista de mensajes de error (vacía = valido)
     */
    public List<String> validate(JsonNode record, IngestionSchema schema) {
        List<String> errors = new ArrayList<>();

        if (schema == null) {
            errors.add("Sin schema para esta institucion");
            return errors;
        }

        for (ValidationRule rule : schema.getValidations()) {
            String field = rule.getField();
            JsonNode value = resolveField(record, field);

            if (rule.isRequired()) {
                if (value == null || value.isNull() || value.isMissingNode()) {
                    errors.add("Campo requerido ausente: " + field);
                    continue;
                }
            }

            if (value != null && !value.isNull() && !value.isMissingNode()) {
                if (rule.getMin() != null && value.isNumber()) {
                    double v = value.asDouble();
                    if (v < rule.getMin()) {
                        errors.add(String.format("Campo %s = %.2f es menor que minimo %.2f",
                                field, v, rule.getMin()));
                    }
                }
                if (rule.getMax() != null && value.isNumber()) {
                    double v = value.asDouble();
                    if (v > rule.getMax()) {
                        errors.add(String.format("Campo %s = %.2f supera maximo %.2f",
                                field, v, rule.getMax()));
                    }
                }
            }
        }

        // Validacion extra: worker.rut debe ser numerico positivo
        JsonNode rut = resolveField(record, "worker.rut");
        if (rut != null && !rut.isNull() && !rut.isMissingNode()) {
            try {
                long rutVal = Long.parseLong(rut.asText().trim());
                if (rutVal <= 0)
                    errors.add("worker.rut debe ser > 0, got: " + rutVal);
            } catch (NumberFormatException e) {
                errors.add("worker.rut no es numerico: " + rut.asText());
            }
        }

        return errors;
    }

    // ── 2. Calculos ──────────────────────────────────────────────────────────

    /**
     * Extrae los valores de contribucion del registro y acumula en el contexto.
     *
     * @param record  nodo JSONL
     * @param schema  schema de la institucion
     * @param context acumulador estadistico del archivo
     */
    public void applyCalculations(JsonNode record, IngestionSchema schema, RendicionContext context) {
        if (schema == null)
            return;

        Set<String> contribFields = schema.getContributionFieldNames();
        JsonNode contributions = record.path("contributions");

        for (String fieldName : contribFields) {
            JsonNode fieldValue = contributions.path(fieldName);
            if (fieldValue != null && !fieldValue.isNull() && !fieldValue.isMissingNode()
                    && fieldValue.isNumber()) {
                context.addContributionSum(fieldName, fieldValue.decimalValue());
            }
        }
    }

    // ── 3. Insercion batch ───────────────────────────────────────────────────

    /**
     * Inserta un lote de registros JSON en MySQL (cotizaciones +
     * cotizacion_contribuciones).
     *
     * <p>
     * Usa INSERT IGNORE para manejar duplicados sin fallar el batch.
     * </p>
     *
     * @param batch   lista de nodos JsonNode del lote
     * @param schema  schema de la institucion
     * @param context contexto para actualizar contadores
     */
    public void batchInsert(List<JsonNode> batch, IngestionSchema schema, RendicionContext context) {
        if (batch == null || batch.isEmpty())
            return;

        String sqlCotizacion = """
                INSERT IGNORE INTO cotizaciones
                  (record_id, institution, format_code, period, source_file,
                   worker_rut, worker_dv, worker_last_name_1, worker_last_name_2, worker_first_name,
                   taxable_income, days_worked, movement_code, normalized_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """;

        String sqlContrib = """
                INSERT INTO cotizacion_contribuciones (cotizacion_id, field_name, field_value)
                VALUES (?,?,?)
                """;

        for (JsonNode record : batch) {
            try {
                JsonNode meta = record.path("_meta");
                JsonNode worker = record.path("worker");

                String recordId = textOrNull(meta, "record_id");
                String institution = textOrNull(meta, "institution");
                String formatCode = textOrNull(meta, "format_code");
                String period = textOrNull(meta, "period");
                String sourceFile = textOrNull(meta, "source_file");

                if (recordId == null || recordId.isBlank()) {
                    context.incrementError();
                    context.addError("record_id nulo en registro");
                    continue;
                }

                long workerRut = longOrZero(worker, "rut");
                String workerDv = textOrNull(worker, "dv");
                String lastName1 = textOrNull(worker, "last_name_1");
                String lastName2 = textOrNull(worker, "last_name_2");
                String firstName = textOrNull(worker, "first_name");
                BigDecimal taxIncome = decimalOrNull(worker, "taxable_income");
                Integer daysWorked = intOrNull(worker, "days_worked");
                String movCode = textOrNull(worker, "movement_code");
                Timestamp normalizedAt = parseTimestamp(textOrNull(meta, "normalized_at"));

                // Intentar insertar la cotizacion principal
                int affected = jdbc.update(sqlCotizacion,
                        recordId, institution, formatCode, period, sourceFile,
                        workerRut, workerDv, lastName1, lastName2, firstName,
                        taxIncome, daysWorked, movCode, normalizedAt);

                if (affected == 0) {
                    // INSERT IGNORE descarto por duplicado
                    context.incrementDuplicate();
                    continue;
                }

                // Obtener el ID generado para insertar contribuciones
                Long cotizacionId = jdbc.queryForObject(
                        "SELECT id FROM cotizaciones WHERE record_id = ?",
                        Long.class, recordId);

                if (cotizacionId != null && schema != null) {
                    JsonNode contribs = record.path("contributions");
                    Set<String> contribFields = schema.getContributionFieldNames();

                    List<Object[]> contribBatch = new ArrayList<>();
                    for (String fieldName : contribFields) {
                        JsonNode val = contribs.path(fieldName);
                        if (!val.isMissingNode() && !val.isNull()) {
                            contribBatch.add(new Object[] {
                                    cotizacionId,
                                    fieldName,
                                    val.decimalValue()
                            });
                        }
                    }
                    if (!contribBatch.isEmpty()) {
                        jdbc.batchUpdate(sqlContrib, contribBatch);
                    }
                }

                context.incrementInserted();

            } catch (Exception e) {
                context.incrementError();
                context.addError("Error insertando registro: " + e.getMessage());
                log.debug("Insert error for record: {}", e.getMessage());
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Resuelve una ruta de campo tipo "worker.rut" o "contributions.afp"
     * en el nodo JSON raiz.
     */
    private JsonNode resolveField(JsonNode root, String fieldPath) {
        if (fieldPath == null || root == null)
            return null;
        String[] parts = fieldPath.split("\\.", 2);
        if (parts.length == 1) {
            return root.path(parts[0]);
        }
        // "worker.rut" → root["worker"]["rut"]
        // "_meta.record_id" → root["_meta"]["record_id"]
        String section = parts[0].equals("_meta") ? "_meta" : parts[0];
        return root.path(section).path(parts[1]);
    }

    private String textOrNull(JsonNode node, String field) {
        JsonNode v = node.path(field);
        return (v.isMissingNode() || v.isNull()) ? null : v.asText(null);
    }

    private long longOrZero(JsonNode node, String field) {
        JsonNode v = node.path(field);
        return (v.isMissingNode() || v.isNull()) ? 0L : v.asLong(0);
    }

    private Integer intOrNull(JsonNode node, String field) {
        JsonNode v = node.path(field);
        return (v.isMissingNode() || v.isNull()) ? null : v.asInt();
    }

    private BigDecimal decimalOrNull(JsonNode node, String field) {
        JsonNode v = node.path(field);
        return (v.isMissingNode() || v.isNull() || !v.isNumber()) ? null : v.decimalValue();
    }

    private Timestamp parseTimestamp(String iso) {
        if (iso == null || iso.isBlank())
            return null;
        try {
            return Timestamp.from(Instant.parse(iso));
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
