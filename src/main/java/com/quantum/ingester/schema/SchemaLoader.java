package com.quantum.ingester.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Carga todos los schemas de ingestion desde classpath:/schemas/*.json
 * al arrancar el servicio. Los schemas provienen del jar de quantum-common.
 *
 * <p>
 * Uso: schemaLoader.getSchema("AFP") → IngestionSchema
 * </p>
 */
@Service
public class SchemaLoader {

    private static final Logger log = LoggerFactory.getLogger(SchemaLoader.class);

    private final ObjectMapper objectMapper;
    private Map<String, IngestionSchema> schemas = Collections.emptyMap();

    public SchemaLoader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void loadSchemas() {
        Map<String, IngestionSchema> loaded = new HashMap<>();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        try {
            Resource[] resources = resolver.getResources("classpath*:schemas/*.json");
            for (Resource r : resources) {
                String filename = r.getFilename();
                if (filename == null || filename.equals("canonical-output-schema.json")) {
                    continue;
                }
                try (InputStream is = r.getInputStream()) {
                    IngestionSchema schema = objectMapper.readValue(is, IngestionSchema.class);
                    if (schema.getInstitution() != null) {
                        loaded.put(schema.getInstitution().toUpperCase(), schema);
                        log.debug("Loaded ingestion schema: {} ({} validations, {} contribution fields)",
                                schema.getInstitution(),
                                schema.getValidations().size(),
                                schema.getContributionFieldNames().size());
                    }
                } catch (IOException e) {
                    log.warn("Could not parse ingestion schema file {}: {}", filename, e.getMessage());
                }
            }
        } catch (IOException e) {
            log.error("Failed to scan classpath schemas: {}", e.getMessage());
        }
        this.schemas = Collections.unmodifiableMap(loaded);
        log.info("Ingestion schemas loaded: {} institutions → {}",
                schemas.size(), schemas.keySet());
    }

    /**
     * Obtiene el schema para una institucion (case-insensitive).
     *
     * @param institution nombre de la institucion (ej: "AFP", "AFC")
     * @return IngestionSchema o null si no existe
     */
    public IngestionSchema getSchema(String institution) {
        if (institution == null)
            return null;
        return schemas.get(institution.toUpperCase());
    }

    /** True si hay schema para la institucion dada. */
    public boolean hasSchema(String institution) {
        return institution != null && schemas.containsKey(institution.toUpperCase());
    }
}
