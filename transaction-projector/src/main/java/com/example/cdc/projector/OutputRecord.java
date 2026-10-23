package com.example.cdc.projector;

import java.io.Serializable;

public record OutputRecord(String topic, String key, String value) implements Serializable {}
