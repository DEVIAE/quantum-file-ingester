package com.quantum.ingester.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Schema de ingestion: subconjunto del schema JSON de quantum-common
 * con solo los campos que necesita el file-ingester:
 * - institution
 * - field_mapping (para derivar los campos de contributions)
 * - validations (array de ValidationRule)
 *
 * <p>
 * Se carga desde los mismos JSON en classpath:/schemas/ (herencia via
 * quantum-common).
 * </p>
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class IngestionSchema {

    private String institution;

    @JsonProperty("filename_pattern")
    private String filenamePattern;

    /**
     * Mapa campo_fuente → ruta_canonica (worker.*, contributions.*, ext.*, _meta.*)
     */
    @JsonProperty("field_mapping")
    private Map<String, String> fieldMapping = Collections.emptyMap();

    /** Reglas de validacion definidas en el schema JSON. */
    private List<ValidationRule> validations = Collections.emptyList();

    /**
     * Devuelve el conjunto de nombres de campos de contribuciones
     * (solo la parte despues de "contributions.").
     * Por ejemplo: {"afp", "sis"} para AFP.
     */
    public Set<String> getContributionFieldNames() {
        return fieldMapping.values().stream()
                .filter(v -> v != null && v.startsWith("contributions."))
                .map(v -> v.substring("contributions.".length()))
                .collect(Collectors.toSet());
    }
}
