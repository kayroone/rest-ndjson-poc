package de.jwiegmann.ndjson.control;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.jwiegmann.ndjson.boundary.dto.ExamplePayload;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Service zur Verarbeitung von NDJSON-Zeilen, gruppiert nach examplePayloadId.
 */
@Service
public class ExampleImportService {

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Liest einen NDJSON-Stream zeilenweise ein, gruppiert die Daten nach examplePayloadId
     * und übergibt sie zur Verarbeitung.
     *
     * @param inputStream der Eingabestrom mit NDJSON-Zeilen
     * @throws IOException bei Problemen beim Lesen des Streams
     */
    public void processNdjsonStream(InputStream inputStream) throws IOException {
        Map<String, List<ExamplePayload>> groupedData = new LinkedHashMap<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            int lineNumber = 1;

            while ((line = reader.readLine()) != null) {
                try {
                    JsonNode node = mapper.readTree(line);

                    if (node.has("header")) {
                        continue;
                    }

                    String id = node.get("examplePayloadId").asText();
                    JsonNode payloadNode = node.get("examplePayload");

                    ExamplePayload payload = mapper.treeToValue(payloadNode, ExamplePayload.class);

                    groupedData.computeIfAbsent(id, k -> new ArrayList<>()).add(payload);

                } catch (Exception e) {
                    System.err.printf("Parsing-Fehler in Zeile %d: %s%n", lineNumber, e.getMessage());
                }

                lineNumber++;
            }

            for (Map.Entry<String, List<ExamplePayload>> entry : groupedData.entrySet()) {
                String id = entry.getKey();
                List<ExamplePayload> payloads = entry.getValue();

                long start = System.currentTimeMillis();

                try {
                    processExamplePayload(id, payloads);
                    long duration = System.currentTimeMillis() - start;
                    System.out.printf("examplePayloadId %s verarbeitet in %d ms (%d Zeilen)%n", id, duration, payloads.size());
                } catch (Exception ex) {
                    System.err.printf("Fehler bei Verarbeitung von examplePayloadId %s: %s%n", id, ex.getMessage());
                }
            }
        }
    }

    /**
     * Verarbeitet eine Liste von Payloads, die zu einer examplePayloadId gehören.
     * Löst bei bestimmten Beträgen (z. B. -9999) absichtlich einen Fehler aus.
     *
     * @param id       die eindeutige Payload-Gruppe
     * @param payloads die zugehörigen Payload-Einträge
     */
    private void processExamplePayload(String id, List<ExamplePayload> payloads) {
        for (ExamplePayload payload : payloads) {
            if (payload.getBetrag() == -9999) {
                throw new RuntimeException("Simulierter Fehler bei examplePayloadId: " + id);
            }
        }
    }
}
