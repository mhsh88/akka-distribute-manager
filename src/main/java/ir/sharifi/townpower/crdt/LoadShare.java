package ir.sharifi.townpower.crdt;

import java.time.Duration;
import java.util.stream.Collectors;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;
import org.apache.pekko.cluster.ddata.Key;
import org.apache.pekko.cluster.ddata.LWWMap;
import org.apache.pekko.cluster.ddata.LWWMapKey;
import org.apache.pekko.cluster.ddata.SelfUniqueAddress;
import org.apache.pekko.cluster.ddata.typed.javadsl.DistributedData;
import org.apache.pekko.cluster.ddata.typed.javadsl.Replicator;
import org.apache.pekko.cluster.ddata.typed.javadsl.ReplicatorMessageAdapter;

import ir.sharifi.townpower.ActorMetrics;
import ir.sharifi.townpower.ActorProfile;
import ir.sharifi.townpower.ActorTrace;
import ir.sharifi.townpower.DescribedActor;
import ir.sharifi.townpower.http.UiHub;

/**
 * Per-node load vote via LWWMap CRDT — Actor Lab merge arcs.
 * shortcut: value = local tell budget this window, not true MW; upgrade to district MW map later.
 */
public final class LoadShare extends AbstractBehavior<LoadShare.Command> implements DescribedActor {

	public interface Command {
	}

	@Override
	public ActorProfile profile() {
		return ActorProfile.LOAD_SHARE;
	}

	public record LocalLoad(int tells) implements Command {
	}

	private enum Flush implements Command {
		INSTANCE
	}

	private record InternalUpdateResponse(Replicator.UpdateResponse<LWWMap<String, Integer>> rsp)
			implements Command {
	}

	private record InternalSubscribeResponse(Replicator.SubscribeResponse<LWWMap<String, Integer>> rsp)
			implements Command {
	}

	private static final Key<LWWMap<String, Integer>> KEY = LWWMapKey.create("node-load");

	private final ActorRef<UiHub.Command> uiHub;
	private final SelfUniqueAddress node;
	private final String memberId;
	private final ReplicatorMessageAdapter<Command, LWWMap<String, Integer>> replicatorAdapter;
	private int pendingTells;
	private String lastDetail = "";

	public static Behavior<Command> create(ActorRef<UiHub.Command> uiHub) {
		return Behaviors.setup(ctx -> Behaviors.withTimers(timers -> new LoadShare(ctx, timers, uiHub)));
	}

	private LoadShare(ActorContext<Command> ctx, TimerScheduler<Command> timers, ActorRef<UiHub.Command> uiHub) {
		super(ctx);
		this.uiHub = uiHub;
		DistributedData ddata = DistributedData.get(ctx.getSystem());
		this.node = ddata.selfUniqueAddress();
		this.memberId = ctx.getSystem().address().toString();
		this.replicatorAdapter = new ReplicatorMessageAdapter<>(ctx, ddata.replicator(), Duration.ofSeconds(3));
		replicatorAdapter.subscribe(KEY, InternalSubscribeResponse::new);
		timers.startTimerWithFixedDelay(Flush.INSTANCE, Duration.ofSeconds(2));
		ActorMetrics.ensureSystem(profile(), memberId);
		ctx.getLog().info("LoadShare CRDT ready key={}", KEY.id());
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder()
				.onMessage(LocalLoad.class, this::onLocal)
				.onMessageEquals(Flush.INSTANCE, this::onFlush)
				.onMessage(InternalUpdateResponse.class, this::onUpdate)
				.onMessage(InternalSubscribeResponse.class, this::onChanged)
				.build();
	}

	private Behavior<Command> onLocal(LocalLoad msg) {
		pendingTells += Math.max(0, msg.tells());
		return this;
	}

	private Behavior<Command> onFlush() {
		int value = pendingTells;
		pendingTells = 0;
		String selfKey = shortMember(memberId);
		replicatorAdapter.askUpdate(
				askReplyTo -> new Replicator.Update<>(
						KEY,
						LWWMap.create(),
						Replicator.writeLocal(),
						askReplyTo,
						curr -> curr.put(node, selfKey, value)),
				InternalUpdateResponse::new);
		uiHub.tell(new UiHub.Trace(ActorTrace.crdt("put", selfKey + "=" + value, memberId)));
		return this;
	}

	private Behavior<Command> onUpdate(InternalUpdateResponse msg) {
		return this;
	}

	private Behavior<Command> onChanged(InternalSubscribeResponse msg) {
		Replicator.SubscribeResponse<LWWMap<String, Integer>> rsp = msg.rsp();
		if (rsp instanceof Replicator.Changed<LWWMap<String, Integer>> changed) {
			LWWMap<String, Integer> map = changed.get(KEY);
			String detail = map.getEntries().entrySet().stream()
					.sorted((a, b) -> a.getKey().compareTo(b.getKey()))
					.map(e -> e.getKey() + "=" + e.getValue())
					.collect(Collectors.joining(","));
			if (!detail.equals(lastDetail) && !detail.isBlank()) {
				lastDetail = detail;
				uiHub.tell(new UiHub.Trace(ActorTrace.crdt("merge", detail, memberId)));
			}
		}
		return this;
	}

	private static String shortMember(String m) {
		int at = m.indexOf('@');
		return at >= 0 ? m.substring(at + 1) : m;
	}
}
