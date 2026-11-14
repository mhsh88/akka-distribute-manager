package ir.sharifi.townpower;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import ir.sharifi.townpower.house.HouseProtocol.Status;

/**
 * Local-node proxy stats for live actors.
 * shortcut: not OS RAM/CPU — msg rate + handleNs + rough state bytes; upgrade with Pekko telemetry.
 */
public final class ActorMetrics {

	private ActorMetrics() {
	}

	public record Snap(
			String kind,
			String title,
			String role,
			List<String> handles,
			String id,
			String name,
			int districtId,
			String memberId,
			String shardId,
			String status,
			double watts,
			long msgCount,
			long totalHandleNs,
			long lastMsgAt,
			long approxStateBytes) {
	}

	private static final ConcurrentHashMap<String, Holder> LOCAL = new ConcurrentHashMap<>();

	public static void record(
			ActorProfile profile,
			String id,
			String name,
			int districtId,
			String memberId,
			String shardId,
			Status status,
			double watts,
			long handleNs) {
		ActorProfile p = profile == null ? ActorProfile.HOUSE : profile;
		String key = p.kind() + "|" + id;
		Holder h = LOCAL.computeIfAbsent(key, k -> new Holder(p, id, name, districtId, memberId, shardId));
		h.profile = p;
		h.name = name;
		h.districtId = districtId;
		h.memberId = memberId;
		h.shardId = shardId;
		h.status = status == null ? Status.ONLINE : status;
		h.watts = watts;
		h.msgCount.incrementAndGet();
		h.totalHandleNs.addAndGet(Math.max(0, handleNs));
		h.lastMsgAt = System.currentTimeMillis();
		h.approxStateBytes = estimateBytes(id, name, memberId, shardId);
	}

	/** Presence row for system actors (Grid/UiHub/Meter/LoadShare) without message flood. */
	public static void ensureSystem(ActorProfile profile, String memberId) {
		if (profile == null) {
			return;
		}
		String id = profile.kind();
		String key = profile.kind() + "|" + id;
		Holder h = LOCAL.computeIfAbsent(key, k -> new Holder(profile, id, profile.title(), 0, memberId, "-"));
		h.profile = profile;
		h.name = profile.title();
		h.memberId = memberId == null ? "" : memberId;
		h.shardId = "-";
		h.status = Status.ONLINE;
		if (h.lastMsgAt == 0) {
			h.lastMsgAt = System.currentTimeMillis();
		}
		h.approxStateBytes = estimateBytes(id, profile.title(), memberId, "-");
	}

	public static void remove(String id) {
		LOCAL.remove(ActorProfile.HOUSE.kind() + "|" + id);
	}

	public static List<Snap> snapshot() {
		List<Snap> out = new ArrayList<>(LOCAL.size());
		for (Holder h : LOCAL.values()) {
			out.add(h.toSnap());
		}
		out.sort(Comparator.comparingLong(Snap::msgCount).reversed());
		return out;
	}

	public static int size() {
		return LOCAL.size();
	}

	private static long estimateBytes(String id, String name, String memberId, String shardId) {
		long chars = len(id) + len(name) + len(memberId) + len(shardId);
		return 96L + chars * 2L;
	}

	private static int len(String s) {
		return s == null ? 0 : s.length();
	}

	private static final class Holder {
		volatile ActorProfile profile;
		final String id;
		volatile String name;
		volatile int districtId;
		volatile String memberId;
		volatile String shardId;
		volatile Status status = Status.ONLINE;
		volatile double watts;
		final AtomicLong msgCount = new AtomicLong();
		final AtomicLong totalHandleNs = new AtomicLong();
		volatile long lastMsgAt;
		volatile long approxStateBytes;

		Holder(ActorProfile profile, String id, String name, int districtId, String memberId, String shardId) {
			this.profile = profile;
			this.id = id;
			this.name = name;
			this.districtId = districtId;
			this.memberId = memberId;
			this.shardId = shardId;
		}

		Snap toSnap() {
			ActorProfile p = profile == null ? ActorProfile.HOUSE : profile;
			return new Snap(
					p.kind(),
					p.title(),
					p.role(),
					p.handles(),
					id,
					name,
					districtId,
					memberId,
					shardId,
					status.name(),
					watts,
					msgCount.get(),
					totalHandleNs.get(),
					lastMsgAt,
					approxStateBytes);
		}
	}
}
