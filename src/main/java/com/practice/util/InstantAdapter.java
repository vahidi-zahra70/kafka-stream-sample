package com.practice.util;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;

public class InstantAdapter extends TypeAdapter<Instant> {

    DateTimeFormatter ISO_INSTANT_FORMATTER = new DateTimeFormatterBuilder().appendInstant(0).toFormatter();

    @Override
    public void write(JsonWriter out, Instant value) throws IOException {
        if (value == null) {
            out.nullValue();
        } else {
            out.value(ISO_INSTANT_FORMATTER.format(value.truncatedTo(ChronoUnit.MILLIS)));
        }
    }

    @Override
    public Instant read(JsonReader in) throws IOException {
        if (in.peek() == JsonToken.NULL) {
            in.nextNull();
            return null;
        } else {
            return Instant.parse(in.nextString());
        }
    }
}
