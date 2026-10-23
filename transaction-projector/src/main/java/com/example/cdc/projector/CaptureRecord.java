package com.example.cdc.projector;

import java.io.Serializable;

public record CaptureRecord(String topic, int partition, long offset, long timestamp, String value)
        implements Serializable {}
