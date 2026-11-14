package ir.sharifi.townpower.house;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;

import java.util.concurrent.ThreadLocalRandom;

import ir.sharifi.townpower.ActorMetrics;
import ir.sharifi.townpower.ActorTrace;
import ir.sharifi.townpower.NeighborhoodCatalog;
import ir.sharifi.townpower.NeighborhoodCatalog.Neighborhood;
import ir.sharifi.townpower.grid.GridProtocol;
import ir.sharifi.townpower.house.HouseProtocol.Blackout;
import ir.sharifi.townpower.house.HouseProtocol.Command;
import ir.sharifi.townpower.house.HouseProtocol.ConsumeWatts;
import ir.sharifi.townpower.house.HouseProtocol.GetStatus;
import ir.sharifi.townpower.house.HouseProtocol.HouseEvent;
import ir.sharifi.townpower.house.HouseProtocol.Passivate;
import ir.sharifi.townpower.house.HouseProtocol.Restore;
import ir.sharifi.townpower.house.HouseProtocol.Status;
import ir.sharifi.townpower.http.UiHub;

/** Sharded entity for one Tehran محله (neighborhood). */
public final class HouseActor extends AbstractBehavior<Command> {

	public static final EntityTypeKey<Command> TYPE_KEY =
			EntityTypeKey.create(Command.class, "Neighborhood");

	private final String neighborhoodId;
	private final String name;
	private final int districtId;
	private final double lat;
	private final double lng;
	private final String shardId;
	private final String memberId;
	private final ActorRef<UiHub.Command> uiHub;
	private final ActorRef<GridProtocol.Command> grid;

	private double watts;
	private Status status = Status.ONLINE;

	public static Behavior<Command> create(
			String neighborhoodId,
			ActorRef<UiHub.Command> uiHub,
			ActorRef<GridProtocol.Command> grid) {
		return Behaviors.setup(ctx -> new HouseActor(ctx, neighborhoodId, uiHub, grid));
	}

	private HouseActor(
			ActorContext<Command> ctx,
			String neighborhoodId,
			ActorRef<UiHub.Command> uiHub,
			ActorRef<GridProtocol.Command> grid) {
		super(ctx);
		this.neighborhoodId = neighborhoodId;
		Neighborhood meta = NeighborhoodCatalog.get().get(neighborhoodId);
		if (meta == null) {
			this.name = neighborhoodId;
			this.districtId = 1;
			this.lat = 35.6892;
			this.lng = 51.3890;
		} else {
			this.name = meta.name();
			this.districtId = meta.districtId();
			this.lat = meta.lat();
			this.lng = meta.lng();
		}
		int shards = ctx.getSystem().settings().config().getInt("pekko.cluster.sharding.number-of-shards");
		this.shardId = String.valueOf(Math.floorMod(neighborhoodId.hashCode(), shards));
		this.memberId = ctx.getSystem().address().toString();
		this.uiHub = uiHub;
		this.grid = grid;
		long t0 = System.nanoTime();
		publish("wake");
		touch(t0);
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder()
				.onMessage(ConsumeWatts.class, this::onConsume)
				.onMessage(Blackout.class, this::onBlackout)
				.onMessage(Restore.class, this::onRestore)
				.onMessage(GetStatus.class, this::onGetStatus)
				.onMessageEquals(Passivate.INSTANCE, this::onPassivate)
				.build();
	}

	private Behavior<Command> onConsume(ConsumeWatts msg) {
		long t0 = System.nanoTime();
		if (status == Status.BLACKOUT || status == Status.SHED) {
			watts = 0;
		} else {
			watts = Math.max(0, msg.watts());
		}
		// sample tell Meter → House for Actor Lab radar (~8%)
		if (ThreadLocalRandom.current().nextInt(100) < 8) {
			uiHub.tell(new UiHub.Trace(ActorTrace.tell(
					"MeterSimulator",
					"house:" + neighborhoodId,
					"ConsumeWatts",
					neighborhoodId,
					districtId,
					lat,
					lng,
					memberId)));
		}
		publish("ping");
		touch(t0);
		return this;
	}

	private Behavior<Command> onBlackout(Blackout msg) {
		long t0 = System.nanoTime();
		String reason = msg.reason() == null ? "" : msg.reason();
		boolean shed = reason.startsWith("manual") || reason.startsWith("supervisor");
		status = shed ? Status.SHED : Status.BLACKOUT;
		watts = 0;
		publish(shed ? "shed" : "blackout");
		touch(t0);
		return this;
	}

	private Behavior<Command> onRestore(Restore msg) {
		long t0 = System.nanoTime();
		status = Status.ONLINE;
		publish("restore");
		touch(t0);
		return this;
	}

	private Behavior<Command> onGetStatus(GetStatus msg) {
		long t0 = System.nanoTime();
		msg.replyTo().tell(new HouseProtocol.StatusView(
				neighborhoodId, name, districtId, watts, status, memberId, shardId));
		touch(t0);
		return this;
	}

	private Behavior<Command> onPassivate() {
		long t0 = System.nanoTime();
		publish("sleep");
		touch(t0);
		ActorMetrics.remove(neighborhoodId);
		return Behaviors.stopped();
	}

	private void touch(long startedNs) {
		ActorMetrics.record(
				neighborhoodId,
				name,
				districtId,
				memberId,
				shardId,
				status,
				watts,
				System.nanoTime() - startedNs);
	}

	private void publish(String action) {
		HouseEvent event = new HouseEvent(
				neighborhoodId, name, districtId, lat, lng, watts, status, action, memberId, shardId);
		uiHub.tell(new UiHub.HouseChanged(event));
		grid.tell(new GridProtocol.HouseTelemetry(neighborhoodId, districtId, watts, status));
		if (!"ping".equals(action)) {
			uiHub.tell(new UiHub.Trace(ActorTrace.life(action, neighborhoodId, districtId, lat, lng, memberId)));
			if ("shed".equals(action) || "blackout".equals(action) || "restore".equals(action)) {
				uiHub.tell(new UiHub.Trace(ActorTrace.tell(
						"GridController",
						"house:" + neighborhoodId,
						action,
						neighborhoodId,
						districtId,
						lat,
						lng,
						memberId)));
			}
		}
	}
}
