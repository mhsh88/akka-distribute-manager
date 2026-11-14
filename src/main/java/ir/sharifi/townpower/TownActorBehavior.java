package ir.sharifi.townpower;

import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;

import ir.sharifi.townpower.house.HouseProtocol.Status;

/**
 * Abstract base for instrumented Town Power actors.
 * Subclasses supply {@link #profile()} and call {@link #noteHandled} after work.
 */
public abstract class TownActorBehavior<T> extends AbstractBehavior<T> implements DescribedActor {

	protected TownActorBehavior(ActorContext<T> ctx) {
		super(ctx);
	}

	/** Register/refresh this instance in local ActorMetrics (proxy busy/RAM). */
	protected final void noteHandled(
			long startedNs,
			String entityId,
			String displayName,
			int districtId,
			String memberId,
			String shardId,
			Status status,
			double watts) {
		ActorMetrics.record(
				profile(),
				entityId,
				displayName,
				districtId,
				memberId,
				shardId,
				status,
				watts,
				System.nanoTime() - startedNs);
	}

	/** One-shot presence for singleton / per-node system actors. */
	protected final void registerPresence(String memberId) {
		ActorMetrics.ensureSystem(profile(), memberId);
	}
}
