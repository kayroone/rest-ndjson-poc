package de.jwiegmann.ndjson.boundary;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Integrationstest für den NDJSON Import mit großen Payloads.
 */
@SpringBootTest
@AutoConfigureMockMvc
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExampleImportControllerIT {

    public static final int ZEILEN_TESTFILE = 50000;
    @Autowired
    private MockMvc mockMvc;

    private Path ndjsonFilePath;

    /**
     * Erzeugt eine temporäre NDJSON-Datei mit ca. 5000 Zeilen Testdaten.
     */
    @BeforeAll
    void generateNdjsonTestFile() throws IOException {

        Path resourceDir = Paths.get("src", "test", "resources", "tmp");
        Files.createDirectories(resourceDir);
        ndjsonFilePath = resourceDir.resolve("example-payload.ndjson");

        try (BufferedWriter writer = Files.newBufferedWriter(ndjsonFilePath)) {

            writer.write("{ \"header\": { \"bewirtschafterNr\": \"123456789\", \"erstellungsdatum\": \"2025-07-04T10:00:00Z\" } }");
            writer.newLine();

            for (int i = 0; i < ZEILEN_TESTFILE; i++) {
                String id = "id" + (i % 100);
                String payload = String.format(
                        "{ \"examplePayloadId\": \"%s\", \"examplePayload\": { \"betrag\": %d, \"zeitstempelWertstellung\": \"2025-07-01T%02d:00:00Z\", \"verwendungszweck\": \"Testzahlung %d\" } }",
                        id, (i + 1) * 10, (i % 24), i + 1
                );
                writer.write(payload);
                writer.newLine();
            }
        }

        System.out.println("NDJSON-Testdatei erzeugt: " + ndjsonFilePath);
    }

    @Test
    void testNdjsonImport_fromFile() throws Exception {
        byte[] fileContent = Files.readAllBytes(ndjsonFilePath);

        mockMvc.perform(post("/api/v1/example-import/upload")
                        .contentType("application/x-ndjson")
                        .content(fileContent))
                .andExpect(status().isOk());
    }

    @Test
    void testNdjsonImport_withIntentionalErrorInPayload() throws Exception {
        Path errorFile = Paths.get("src", "test", "resources", "tmp", "example-error-payload.ndjson");

        try (BufferedWriter writer = Files.newBufferedWriter(errorFile)) {

            // Fehlerhafte Gruppe: enthält Betrag -9999
            writer.write("{ \"examplePayloadId\": \"idFehler\", \"examplePayload\": { \"betrag\": 100, \"zeitstempelWertstellung\": \"2025-07-01T10:00:00Z\", \"verwendungszweck\": \"Valid innerhalb Fehlergruppe\" } }");
            writer.newLine();
            writer.write("{ \"examplePayloadId\": \"idFehler\", \"examplePayload\": { \"betrag\": -9999, \"zeitstempelWertstellung\": \"2025-07-01T10:05:00Z\", \"verwendungszweck\": \"FEHLER AUSLÖSEN\" } }");
            writer.newLine();

            // Gültige Gruppe: sollte verarbeitet werden
            writer.write("{ \"examplePayloadId\": \"idOK\", \"examplePayload\": { \"betrag\": 12345, \"zeitstempelWertstellung\": \"2025-07-01T11:00:00Z\", \"verwendungszweck\": \"Alles gut\" } }");
            writer.newLine();
        }

        byte[] content = Files.readAllBytes(errorFile);

        mockMvc.perform(post("/api/v1/example-import/upload")
                        .contentType("application/x-ndjson")
                        .content(content))
                .andExpect(status().isOk());
    }
}
