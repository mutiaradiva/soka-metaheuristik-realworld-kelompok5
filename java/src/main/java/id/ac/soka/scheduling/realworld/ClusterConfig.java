package id.ac.soka.scheduling.realworld;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.List;

public record ClusterConfig(List<Worker> workers) {
    public record Worker(String hostId, int vmId, String endpoint, int slots, double modeledHourlyRateUsd) {}
    public static ClusterConfig load(Path path) throws Exception {
        ClusterConfig c = new ObjectMapper().readValue(path.toFile(), ClusterConfig.class);
        if (c.workers() == null || c.workers().isEmpty()) throw new IllegalArgumentException("Cluster config has no workers");
        for (Worker w : c.workers()) if (w.hostId() == null || w.endpoint() == null || w.slots() < 1 || w.modeledHourlyRateUsd() < 0) throw new IllegalArgumentException("Invalid worker entry: " + w);
        return c;
    }
}
