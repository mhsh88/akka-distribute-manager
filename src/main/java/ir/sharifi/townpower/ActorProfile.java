package ir.sharifi.townpower;

import java.util.List;

/** Shared role card for every Town Power actor kind (shown on UI hover). */
public record ActorProfile(
		String kind,
		String title,
		String role,
		List<String> handles) {

	public static final ActorProfile HOUSE = new ActorProfile(
			"HouseActor",
			"محله / House entity",
			"Sharded entity for one Tehran neighborhood. Holds live load (watts) and power status; "
					+ "publishes telemetry to GridController and UI hub.",
			List.of(
					"ConsumeWatts — meter tick updates watts",
					"Blackout — force dark (grid / critical)",
					"Restore — back online",
					"GetStatus — ask current view",
					"Passivate — sleep / hand back shard"));

	public static final ActorProfile GRID = new ActorProfile(
			"GridController",
			"ناظر شبکه / Grid singleton",
			"Cluster singleton DSM supervisor. Aggregates district load from house telemetry; "
					+ "trips overload / critical sites; issues shed/blackout/restore.",
			List.of(
					"HouseTelemetry — roll up district MW",
					"SetStress — heatwave scenario",
					"Tick — periodic supervision",
					"DelayedCriticalTrip — staged critical outage",
					"StatusReply / AskTimeout — house ask flow"));

	public static final ActorProfile METER = new ActorProfile(
			"MeterSimulator",
			"شبیه‌ساز کنتور / Meter",
			"Per-node load generator. Pushes ConsumeWatts into sharded houses on a timer; "
					+ "respects stress/hour; feeds LoadShare CRDT.",
			List.of(
					"internal timer ticks — drive consumption",
					"SetStress — amplify load pattern",
					"SetHour — clock / scenario hour",
					"StreamDrop — backpressure signal"));

	public static final ActorProfile UI_HUB = new ActorProfile(
			"UiHub",
			"هاب UI / fan-out",
			"Per-node UI bridge. Receives house/grid/trace events, mirrors to peer hubs, "
					+ "and drives HttpGateway WebSocket/JSON snapshots.",
			List.of(
					"HouseChanged / RemoteHouse — neighborhood state",
					"GridSnapshot — district sums / blackouts",
					"Trace / RemoteTrace — Actor Lab radar",
					"SetStress / SetHour / Shed / Restore — UI commands",
					"cluster Member/Reach/Listing adapters"));

	public static final ActorProfile LOAD_SHARE = new ActorProfile(
			"LoadShare",
			"CRDT بار / LoadShare",
			"Replicated load share via Pekko Distributed Data. Merges per-node contributions "
					+ "so every node sees a cluster-wide load hint.",
			List.of(
					"LocalLoad — publish this node’s value",
					"Flush — push timer",
					"UpdateResponse / SubscribeResponse — DData callbacks"));

	public static List<ActorProfile> all() {
		return List.of(HOUSE, GRID, METER, UI_HUB, LOAD_SHARE);
	}

	public static ActorProfile byKind(String kind) {
		if (kind == null) {
			return HOUSE;
		}
		for (ActorProfile p : all()) {
			if (p.kind.equals(kind)) {
				return p;
			}
		}
		return HOUSE;
	}
}
