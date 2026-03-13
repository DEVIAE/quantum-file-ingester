package com.quantum.ingester.generator;

import com.quantum.ingester.model.RendicionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Genera el archivo de rendicion (.ren) al finalizar el procesamiento de un
 * archivo JSONL.
 *
 * <p>
 * El archivo se escribe en el outputDir del ingester con el mismo nombre base
 * del JSONL pero con extension .ren.<br>
 * Ejemplo: AFC_076354771K_202502_01.jsonl → AFC_076354771K_202502_01.ren
 * </p>
 *
 * <p>
 * Formato de salida:
 * </p>
 * 
 * <pre>
 * ========================================
 * RENDICION DE PROCESAMIENTO
 * Archivo  : AFC_076354771K_202502_01.jsonl
 * Fecha    : 2026-03-12T15:06:46Z
 * ========================================
 * [VALIDACION]
 *   Registros leidos    :  1,981,069
 *   Registros validos   :  1,981,069
 *   Registros con error :          0
 *   Campos validados    : worker.rut, worker.taxable_income, contributions.*
 *
 * [CALCULOS - Sumas de contribuciones]
 *   afc_worker          :  1,234,567,890.00
 *   afc_employer        :    987,654,321.00
 *   afc_total           :  2,222,222,211.00
 *
 * [REGISTROS]
 *   Total leidos        :  1,981,069
 *   Omitidos (errores)  :          0
 *   Procesados          :  1,981,069
 *
 * [BASE DE DATOS]
 *   Insertados          :  1,981,069
 *   Duplicados          :          0
 *   Errores             :          0
 * ========================================
 * RESULTADO: OK
 * Tiempo de procesamiento: 4,521 ms
 * ========================================
 * </pre>
 */
@Component
public class RendicionGenerator {

    private static final Logger log = LoggerFactory.getLogger(RendicionGenerator.class);

    private static final String SEP = "========================================";
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
            .withZone(ZoneId.of("UTC"));

    /**
     * Genera el archivo .ren y lo escribe en outputDir.
     *
     * @param outputDir directorio donde se escribe el .ren (ej: /data/output)
     * @param context   contexto con todas las estadisticas del archivo
     * @return Path del archivo .ren generado, o null si fallo
     */
    public Path generate(String outputDir, RendicionContext context) {
        String baseName = stripExtension(context.getSourceFile());
        String renFileName = baseName + ".ren";
        Path renPath = Paths.get(outputDir, renFileName);

        try {
            Files.createDirectories(renPath.getParent());
        } catch (IOException e) {
            log.error("No se pudo crear directorio de salida {}: {}", outputDir, e.getMessage());
            return null;
        }

        try (PrintWriter pw = new PrintWriter(
                Files.newBufferedWriter(renPath, StandardCharsets.UTF_8))) {

            writeHeader(pw, context);
            writeValidationSection(pw, context);
            writeCalculosSection(pw, context);
            writeRegistrosSection(pw, context);
            writeBaseDatosSection(pw, context);
            writeFooter(pw, context);

        } catch (IOException e) {
            log.error("Error escribiendo archivo .ren {}: {}", renPath, e.getMessage());
            return null;
        }

        log.info("[RendicionGenerator] Generado: {} ({})", renPath.getFileName(), context.getResult());
        return renPath;
    }

    // ── Secciones ─────────────────────────────────────────────────────────────

    private void writeHeader(PrintWriter pw, RendicionContext ctx) {
        pw.println(SEP);
        pw.println("RENDICION DE PROCESAMIENTO");
        pw.printf("Archivo    : %s%n", ctx.getSourceFile());
        pw.printf("Institucion: %s%n", ctx.getInstitution());
        pw.printf("Periodo    : %s%n", ctx.getPeriod());
        pw.printf("Fecha      : %s%n", FMT.format(Instant.now()));
        pw.println(SEP);
        pw.println();
    }

    private void writeValidationSection(PrintWriter pw, RendicionContext ctx) {
        pw.println("[VALIDACION]");
        pw.printf("  %-26s: %,15d%n", "Registros leidos", ctx.getRecordsRead());
        pw.printf("  %-26s: %,15d%n", "Registros validos", ctx.getRecordsValid());
        pw.printf("  %-26s: %,15d%n", "Registros con error", ctx.getRecordsError());

        if (!ctx.getErrorSample().isEmpty()) {
            pw.println();
            pw.println("  Muestra de errores (primeros " + ctx.getErrorSample().size() + "):");
            for (String err : ctx.getErrorSample()) {
                pw.printf("    - %s%n", err);
            }
        }
        pw.println();
    }

    private void writeCalculosSection(PrintWriter pw, RendicionContext ctx) {
        pw.println("[CALCULOS - Sumas de contribuciones]");
        Map<String, BigDecimal> sums = new TreeMap<>(ctx.getContributionSums());
        if (sums.isEmpty()) {
            pw.println("  (sin contribuciones calculadas)");
        } else {
            for (Map.Entry<String, BigDecimal> e : sums.entrySet()) {
                pw.printf("  %-30s: %,20.2f%n", e.getKey(), e.getValue());
            }
        }
        pw.println();
    }

    private void writeRegistrosSection(PrintWriter pw, RendicionContext ctx) {
        pw.println("[REGISTROS]");
        pw.printf("  %-26s: %,15d%n", "Total leidos", ctx.getRecordsRead());
        pw.printf("  %-26s: %,15d%n", "Omitidos (errores)", ctx.getRecordsError());
        pw.printf("  %-26s: %,15d%n", "Procesados", ctx.getRecordsValid());
        pw.println();
    }

    private void writeBaseDatosSection(PrintWriter pw, RendicionContext ctx) {
        pw.println("[BASE DE DATOS]");
        pw.printf("  %-26s: %,15d%n", "Insertados", ctx.getRecordsInserted());
        pw.printf("  %-26s: %,15d%n", "Duplicados", ctx.getRecordsDuplicate());
        pw.printf("  %-26s: %,15d%n", "Errores DB",
                ctx.getRecordsValid() - ctx.getRecordsInserted() - ctx.getRecordsDuplicate());
        pw.println();
    }

    private void writeFooter(PrintWriter pw, RendicionContext ctx) {
        pw.println(SEP);
        pw.printf("RESULTADO: %s%n", ctx.getResult());
        pw.printf("Tiempo de procesamiento: %,d ms%n", ctx.getProcessingTimeMs());
        pw.println(SEP);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String stripExtension(String filename) {
        if (filename == null)
            return "unknown";
        int dotIdx = filename.lastIndexOf('.');
        // remove trailing path separators
        int sepIdx = Math.max(filename.lastIndexOf('/'), filename.lastIndexOf('\\'));
        String base = sepIdx >= 0 ? filename.substring(sepIdx + 1) : filename;
        dotIdx = base.lastIndexOf('.');
        return dotIdx > 0 ? base.substring(0, dotIdx) : base;
    }
}
