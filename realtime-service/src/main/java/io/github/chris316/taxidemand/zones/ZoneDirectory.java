package io.github.chris316.taxidemand.zones;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

/** In-memory lookup of the 265 TLC taxi zones, loaded once from taxi_zone_lookup.csv. */
@Component
public class ZoneDirectory {

    private final Map<Integer, Zone> zones;

    public ZoneDirectory() {
        this(new ClassPathResource("data/taxi_zone_lookup.csv"));
    }

    ZoneDirectory(ClassPathResource csv) {
        Map<Integer, Zone> loaded = new TreeMap<>();
        try (var reader = new BufferedReader(new InputStreamReader(csv.getInputStream(), StandardCharsets.UTF_8))) {
            reader.readLine(); // header
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                List<String> cols = parseCsvLine(line);
                int id = Integer.parseInt(cols.get(0));
                loaded.put(id, new Zone(id, cols.get(2), cols.get(1), cols.get(3)));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot load taxi zone lookup", e);
        }
        this.zones = Collections.unmodifiableMap(loaded);
    }

    public Zone lookup(int zoneId) {
        Zone z = zones.get(zoneId);
        return z != null ? z : new Zone(zoneId, "Unknown", "Unknown", "Unknown");
    }

    public boolean exists(int zoneId) {
        return zones.containsKey(zoneId);
    }

    public Collection<Zone> all() {
        return zones.values();
    }

    /** Minimal RFC 4180 parsing: quoted fields, commas inside quotes, doubled quotes. */
    static List<String> parseCsvLine(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (quoted) {
                if (c == '"' && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    cur.append('"');
                    i++;
                } else if (c == '"') {
                    quoted = false;
                } else {
                    cur.append(c);
                }
            } else if (c == '"') {
                quoted = true;
            } else if (c == ',') {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        out.add(cur.toString());
        return out;
    }

    public record Zone(int id, String name, String borough, String serviceZone) {
    }
}
