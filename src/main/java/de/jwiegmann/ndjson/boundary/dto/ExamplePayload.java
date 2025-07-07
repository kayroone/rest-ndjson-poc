package de.jwiegmann.ndjson.boundary.dto;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class ExamplePayload {

    private long betrag;
    private String zeitstempelWertstellung;
    private String verwendungszweck;
}