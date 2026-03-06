package com.quantum.ingester.service;

import com.quantum.common.model.Chunk;
import com.quantum.common.util.LoggingUtils;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.apache.hc.core5.http.ParseException;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.util.Map;

@Service
public class ElasticsearchService {

    @Value("${elk.elasticsearch.url}")
    private String elasticUrl;

    @Value("${elk.elasticsearch.api-key}")
    private String apiKey;

    @Value("${elk.elasticsearch.index}")
    private String index;

    public void indexChunk(Chunk chunk) throws IOException {
        // Construir el JSON con los campos que quieras
        String json = """
                {
                    "chunkId": "%s",
                    "fileName": "%s",
                    "chunkIndex": %d,
                    "totalChunks": %d,
                    "lineCount": %d,
                    "timestamp": "%s",
                    "service": "file-ingester",
                    "correlationId": "%s"
                }
                """.formatted(chunk.getChunkId(), chunk.getFileName(), chunk.getChunkIndex(),
                chunk.getTotalChunks(), chunk.getLines().size(), chunk.getTimestamp());

        // Crear la petición HTTP POST
        String endpoint = elasticUrl + "/" + index + "/_doc";

        try (CloseableHttpClient client = HttpClients.createDefault()) {

            HttpPost request = new HttpPost(endpoint);

            request.setHeader("Authorization", "ApiKey " + apiKey);
            request.setHeader("Content-Type", "application/json");

            request.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));

            try (CloseableHttpResponse response = client.execute(request)) {

                String responseBody;

                try {
                    responseBody = EntityUtils.toString(response.getEntity());
                } catch (ParseException e) {
                    throw new IOException("Error parsing Elasticsearch response", e);
                }

                if (response.getCode() != 201) {
                    throw new IOException("Failed to index chunk: " + responseBody);
                }
            }
        }
    }
}