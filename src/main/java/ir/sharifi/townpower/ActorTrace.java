package ir.sharifi.townpower;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Lightweight actor-model breadcrumb for the Actor Lab UI.
 * kind: tell | life | crown | watch | ping | dead | crdt | stream
 */
public record ActorTrace(
		String kind,
		String from,
		String to,
		String action,
		String entityId,
		int districtId,
		double lat,
		double lng,
		String memberId,
		long ts) implements CborSerializable {

	@JsonCreator
	public ActorTrace(
			@JsonProperty("kind") String kind,
			@JsonProperty("from") String from,
			@JsonProperty("to") String to,
			@JsonProperty("action") String action,
			@JsonProperty("entityId") String entityId,
			@JsonProperty("districtId") int districtId,
			@JsonProperty("lat") double lat,
			@JsonProperty("lng") double lng,
			@JsonProperty("memberId") String memberId,
			@JsonProperty("ts") long ts) {
		this.kind = kind;
		this.from = from;
		this.to = to;
		this.action = action;
		this.entityId = entityId;
		this.districtId = districtId;
		this.lat = lat;
		this.lng = lng;
		this.memberId = memberId;
		this.ts = ts;
	}

	public static ActorTrace tell(
			String from,
			String to,
			String action,
			String entityId,
			int districtId,
			double lat,
			double lng,
			String memberId) {
		return new ActorTrace("tell", from, to, action, entityId, districtId, lat, lng, memberId, System.currentTimeMillis());
	}

	public static ActorTrace life(
			String action,
			String entityId,
			int districtId,
			double lat,
			double lng,
			String memberId) {
		return new ActorTrace("life", "system", "house:" + entityId, action, entityId, districtId, lat, lng, memberId, System.currentTimeMillis());
	}

	public static ActorTrace crown(String memberId) {
		return new ActorTrace("crown", "cluster", "GridController", "singleton", "", 0, 0, 0, memberId, System.currentTimeMillis());
	}

	/** Cluster membership / reachability (DeathWatch theater). */
	public static ActorTrace watch(String action, String memberId) {
		return new ActorTrace(
				"watch", "cluster", "membership", action, "", 0, 0, 0, memberId, System.currentTimeMillis());
	}

	/** Ask/reply: action = "ask" or "reply:&lt;ms&gt;". */
	public static ActorTrace ping(
			String action,
			String entityId,
			int districtId,
			double lat,
			double lng,
			String memberId) {
		return new ActorTrace(
				"ping",
				"GridController",
				"house:" + entityId,
				action,
				entityId,
				districtId,
				lat,
				lng,
				memberId,
				System.currentTimeMillis());
	}

	/** Undeliverable message hit deadLetters. */
	public static ActorTrace dead(String messageType, String recipient) {
		return new ActorTrace(
				"dead",
				"sender",
				recipient == null ? "?" : recipient,
				messageType == null ? "?" : messageType,
				"",
				0,
				0,
				0,
				"",
				System.currentTimeMillis());
	}

	/** Distributed Data merge: action=merge|put, entityId=detail payload. */
	public static ActorTrace crdt(String action, String detail, String memberId) {
		return new ActorTrace(
				"crdt",
				"replicator",
				"LWWMap:node-load",
				action,
				detail == null ? "" : detail,
				0,
				0,
				0,
				memberId == null ? "" : memberId,
				System.currentTimeMillis());
	}

	/** Pekko Stream meter signal: action=tick|backpressure|N. */
	public static ActorTrace stream(String action, String memberId) {
		return new ActorTrace(
				"stream",
				"MeterSimulator",
				"houses",
				action,
				"",
				0,
				0,
				0,
				memberId == null ? "" : memberId,
				System.currentTimeMillis());
	}
}
