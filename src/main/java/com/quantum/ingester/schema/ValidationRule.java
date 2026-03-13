package com.quantum.ingester.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

/**
 * Regla de validacion para un campo canonico en el JSONL.
 * Se deserializa desde la seccion "validations" de cada schema JSON.
 * <p>
 * Ejemplos de campo:
 * - "worker.rut"
 * - "contributions.afp"
 * - "ext.debt_total"
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ValidationRule {

    /**
     * Ruta del campo en el CanonicalRecord: "worker.rut", "contributions.afp", etc.
     */
    private String field;

    /** true = el campo no debe ser null ni ausente */
    private boolean required = false;

    /**
     * Valor minimo (inclusive) para campos numericos. null = sin limite inferior.
     */
    private Double min;

    /**
     * Valor maximo (inclusive) para campos numericos. null = sin limite superior.
     */
    private Double max;
}
