package ir.sharifi.townpower;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import ir.sharifi.townpower.house.HouseProtocol.Status;

/**
 * Local-node proxy stats for live house entities.
 * shortcut: not OS RAM/CPU — msg rate + handleNs + rough state bytes; upgrade with Pekko telemetry.
 */
public final class ActorMetrics {

	private ActorMetrics() {
	}

	public record Snap(
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
			String id,
			String name,
			int districtId,
			String memberId,
			String shardId,
			Status status,
			double watts,
			long handleNs) {
		Holder h = LOCAL.computeIfAbsent(id, k -> new Holder(id, name, districtId, memberId, shardId));
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

	public static void remove(String id) {
		LOCAL.remove(id);
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

		Holder(String id, String name, int districtId, String memberId, String shardId) {
			this.id = id;
			this.name = name;
			this.districtId = districtId;
			this.memberId = memberId;
			this.shardId = shardId;
		}

		Snap toSnap() {
			return new Snap(
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
