package ir.sharifi.townpower;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Critical Tehran sites: hospital / metro / stadium — last to blackout, first to restore. */
public final class CriticalSites {

	public enum Kind {
		HOSPITAL, METRO, STADIUM
	}

	public record Site(String id, String displayName, Kind kind, int districtId) {
	}

	private static final CriticalSites INSTANCE = load();

	private final Set<String> ids;
	private final Map<String, Site> byId;

	private CriticalSites(Map<String, Site> byId) {
		this.byId = Map.copyOf(byId);
		this.ids = Set.copyOf(byId.keySet());
	}

	public static CriticalSites get() {
		return INSTANCE;
	}

	public boolean isCritical(String neighborhoodId) {
		return ids.contains(neighborhoodId);
	}

	public Site get(String neighborhoodId) {
		return byId.get(neighborhoodId);
	}

	public Set<String> ids() {
		return ids;
	}

	private static CriticalSites load() {
		try {
			ObjectMapper mapper = new ObjectMapper();
			Map<String, Site> map = new HashMap<>();
			try (InputStream in = CriticalSites.class.getResourceAsStream("/town/critical-sites.json")) {
				JsonNode arr = mapper.readTree(in);
				for (JsonNode n : arr) {
					Kind kind = switch (n.get("type").asText()) {
						case "metro" -> Kind.METRO;
						case "stadium" -> Kind.STADIUM;
						default -> Kind.HOSPITAL;
					};
					String id = n.get("id").asText();
					String label = n.has("displayName") ? n.get("displayName").asText() : n.get("name").asText();
					map.put(id, new Site(id, label, kind, n.get("districtId").asInt()));
				}
			}
			return new CriticalSites(Collections.unmodifiableMap(map));
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load critical sites", e);
		}
	}
}
