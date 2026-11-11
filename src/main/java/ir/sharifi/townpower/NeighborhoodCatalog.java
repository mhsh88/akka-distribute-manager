package ir.sharifi.townpower;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Static Tehran محله catalog (loaded once from classpath). */
public final class NeighborhoodCatalog {

	public record Neighborhood(String id, String name, int districtId, double lat, double lng) {
	}

	private static final NeighborhoodCatalog INSTANCE = load();

	private final List<Neighborhood> all;
	private final Map<String, Neighborhood> byId;
	private final Map<Integer, int[]> neighbors;

	private NeighborhoodCatalog(List<Neighborhood> all, Map<Integer, int[]> neighbors) {
		this.all = List.copyOf(all);
		Map<String, Neighborhood> map = new HashMap<>();
		for (Neighborhood n : all) {
			map.put(n.id(), n);
		}
		this.byId = Map.copyOf(map);
		this.neighbors = neighbors;
	}

	public static NeighborhoodCatalog get() {
		return INSTANCE;
	}

	public List<Neighborhood> all() {
		return all;
	}

	public Neighborhood get(String id) {
		return byId.get(id);
	}

	public int[] neighborsOf(int districtId) {
		return neighbors.getOrDefault(districtId, new int[0]);
	}

	public int districtCount() {
		return 22;
	}

	private static NeighborhoodCatalog load() {
		try {
			ObjectMapper mapper = new ObjectMapper();
			List<Neighborhood> list = new ArrayList<>();
			try (InputStream in = NeighborhoodCatalog.class.getResourceAsStream("/town/neighborhoods.json")) {
				JsonNode arr = mapper.readTree(in);
				for (JsonNode n : arr) {
					list.add(new Neighborhood(
							n.get("id").asText(),
							n.get("name").asText(),
							n.get("districtId").asInt(),
							n.get("lat").asDouble(),
							n.get("lng").asDouble()));
				}
			}
			Map<Integer, int[]> adj = new HashMap<>();
			try (InputStream in = NeighborhoodCatalog.class.getResourceAsStream("/town/district-neighbors.json")) {
				JsonNode obj = mapper.readTree(in);
				obj.fields().forEachRemaining(e -> {
					int[] ns = new int[e.getValue().size()];
					int i = 0;
					for (JsonNode v : e.getValue()) {
						ns[i++] = v.asInt();
					}
					adj.put(Integer.parseInt(e.getKey()), ns);
				});
			}
			return new NeighborhoodCatalog(list, Collections.unmodifiableMap(adj));
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load neighborhood catalog", e);
		}
	}
}
