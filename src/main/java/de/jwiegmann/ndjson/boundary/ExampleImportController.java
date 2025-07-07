package de.jwiegmann.ndjson.boundary;

import de.jwiegmann.ndjson.control.ExampleImportService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;

/**
 * REST-Controller zur Entgegennahme und Verarbeitung von NDJSON-Daten.
 */
@RestController
@RequestMapping("/api/v1/example-import")
public class ExampleImportController {

    private final ExampleImportService exampleImportService;

    public ExampleImportController(ExampleImportService exampleImportService) {
        this.exampleImportService = exampleImportService;
    }

    /**
     * POST-Endpunkt zur Entgegennahme von NDJSON-Zeilen über einen InputStream.
     * Erwartet Content-Type: application/x-ndjson
     *
     * @param inputStream Eingabestrom mit NDJSON-Zeilen
     * @return HTTP 200 bei Erfolg, HTTP 500 bei Fehler
     */
    @PostMapping(value = "/upload", consumes = MediaType.APPLICATION_NDJSON_VALUE)
    public ResponseEntity<String> importExample(InputStream inputStream) {
        try {
            exampleImportService.processNdjsonStream(inputStream);
            return ResponseEntity.ok("Import abgeschlossen.");
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Fehler beim Verarbeiten des NDJSON-Streams: " + e.getMessage());
        }
    }
}
