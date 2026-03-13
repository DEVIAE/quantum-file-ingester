package com.quantum.ingester.model;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Acumulador de estadisticas de procesamiento para un archivo JSONL.
 * Thread-safe para uso en entornos de procesamiento paralelo.
 *
 * <p>
 * Se crea una instancia por archivo procesado, se popula durante
 * el pipeline de validacion/calculo/insercion, y se usa al final
 * para generar el archivo .ren.
 * </p>
 */
public class RendicionContext {

    private final String sourceFile;
    private final String institution;
    private final String period;
    private final long startTimeMs;

    private final AtomicInteger recordsRead = new AtomicInteger(0);
    private final AtomicInteger recordsValid = new AtomicInteger(0);
    private final AtomicInteger recordsError = new AtomicInteger(0);
    private final AtomicInteger recordsInserted = new AtomicInteger(0);
    private final AtomicInteger recordsDuplicate = new AtomicInteger(0);

    /**
     * Suma de cada campo de contribucion. Clave: nombre del campo (sin
     * "contributions.")
     */
    private final ConcurrentHashMap<String, BigDecimal> contributionSums = new ConcurrentHashMap<>();

    /** Primeros N mensajes de error para incluir en el .ren */
    private final List<String> errorSample = new ArrayList<>();
    private static final int MAX_ERROR_SAMPLE = 20;

    public RendicionContext(String sourceFile, String institution, String period) {
        this.sourceFile = sourceFile;
        this.institution = institution;
        this.period = period;
        this.startTimeMs = System.currentTimeMillis();
    }

    // ── Counters ─────────────────────────────────────────────────────────────

    public void incrementRead() {
        recordsRead.incrementAndGet();
    }

    public void incrementValid() {
        recordsValid.incrementAndGet();
    }

    public void incrementError() {
        recordsError.incrementAndGet();
    }

    public void incrementInserted() {
        recordsInserted.incrementAndGet();
    }

    public void incrementDuplicate() {
        recordsDuplicate.incrementAndGet();
    }

    public synchronized void addError(String msg) {
        if (errorSample.size() < MAX_ERROR_SAMPLE) {
            errorSample.add(msg);
        }
    }

    // ── Contribution sums ────────────────────────────────────────────────────

    /**
     * Suma el valor al acumulador del campo indicado.
     *
     * @param fieldName nombre del campo (sin "contributions."), ej: "afp"
     * @param value     valor a sumar (ignorado si null)
     */
    public void addContributionSum(String fieldName, BigDecimal value) {
        if (value == null)
            return;
        contributionSums.merge(fieldName, value, BigDecimal::add);
    }

    // ── Getters ──────────────────────────────────────────────────────────────

    public String getSourceFile() {
        return sourceFile;
    }

    public String getInstitution() {
        return institution;
    }

    public String getPeriod() {
        return period;
    }

    public int getRecordsRead() {
        return recordsRead.get();
    }

    public int getRecordsValid() {
        return recordsValid.get();
    }

    public int getRecordsError() {
        return recordsError.get();
    }

    public int getRecordsInserted() {
        return recordsInserted.get();
    }

    public int getRecordsDuplicate() {
        return recordsDuplicate.get();
    }

    public Map<String, BigDecimal> getContributionSums() {
        return contributionSums;
    }

    public List<String> getErrorSample() {
        return List.copyOf(errorSample);
    }

    public long getProcessingTimeMs() {
        return System.currentTimeMillis() - startTimeMs;
    }

    /** true si no hubo errors de validacion */
    public boolean isFullyValid() {
        return recordsError.get() == 0;
    }

    /**
     * "OK" si ningun error, "PARTIAL" si hay errores pero se completó, "ERROR" si
     * todo fallo
     */
    public String getResult() {
        int read = recordsRead.get();
        int error = recordsError.get();
        if (read == 0)
            return "ERROR";
        if (error == 0)
            return "OK";
        if (error < read)
            return "PARTIAL";
        return "ERROR";
    }
}
