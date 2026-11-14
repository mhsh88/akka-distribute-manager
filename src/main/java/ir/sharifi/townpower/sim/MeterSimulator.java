package ir.sharifi.townpower.sim;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.stream.KillSwitches;
import org.apache.pekko.stream.Materializer;
import org.apache.pekko.stream.OverflowStrategy;
import org.apache.pekko.stream.UniqueKillSwitch;
import org.apache.pekko.stream.javadsl.Keep;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;

import ir.sharifi.townpower.ActorMetrics;
import ir.sharifi.townpower.ActorProfile;
import ir.sharifi.townpower.ActorTrace;
import ir.sharifi.townpower.DescribedActor;
import ir.sharifi.townpower.NeighborhoodCatalog;
import ir.sharifi.townpower.NeighborhoodCatalog.Neighborhood;
import ir.sharifi.townpower.crdt.LoadShare;
import ir.sharifi.townpower.house.HouseActor;
import ir.sharifi.townpower.house.HouseProtocol;
import ir.sharifi.townpower.http.UiHub;

/**
 * Pekko Stream meter: tick → buffer → tell houses; stress → dropHead + backpressure crumbs.
 */
public final class MeterSimulator extends AbstractBehavior<MeterSimulator.Command> implements DescribedActor {

	public interface Command {
	}

	@Override
	public ActorProfile profile() {
		return ActorProfile.METER;
	}

	public record SetStress(boolean enabled) implements Command {
	}

	/** hour in [0, 24); negative = follow wall clock */
	public record SetHour(double hour) implements Command {
	}

	private record StreamDrop(int dropped) implements Command {
	}

	private final ClusterSharding sharding;
	private final List<Neighborhood> neighborhoods;
	private final LoadModel loadModel;
	private final double stressMultiplier;
	private final ActorRef<UiHub.Command> uiHub;
	private final ActorRef<LoadShare.Command> loadShare;
	private final Materializer materializer;
	private final AtomicBoolean stress = new AtomicBoolean(false);
	private final AtomicReference<Double> simHour = new AtomicReference<>(-1.0);
	private final AtomicInteger cursor = new AtomicInteger(0);
	private final AtomicInteger dropWindow = new AtomicInteger(0);
	private UniqueKillSwitch killSwitch;

	public static Behavior<Command> create(ActorRef<UiHub.Command> uiHub, ActorRef<LoadShare.Command> loadShare) {
		return Behaviors.setup(ctx -> new MeterSimulator(ctx, uiHub, loadShare));
	}

	private MeterSimulator(
			ActorContext<Command> ctx, ActorRef<UiHub.Command> uiHub, ActorRef<LoadShare.Command> loadShare) {
		super(ctx);
		this.uiHub = uiHub;
		this.loadShare = loadShare;
		this.sharding = ClusterSharding.get(ctx.getSystem());
		this.neighborhoods = NeighborhoodCatalog.get().all();
		this.materializer = Materializer.createMaterializer(ctx.getSystem());
		var cfg = ctx.getSystem().settings().config().getConfig("town");
		this.stressMultiplier = cfg.getDouble("stress-multiplier");
		this.loadModel = new LoadModel(
				cfg.getDouble("load-base-mw"),
				cfg.getDouble("load-north-boost-mw"),
				cfg.getDouble("load-temp-ref-c"),
				cfg.getDouble("load-temp-alpha"),
				cfg.getDouble("load-noise-sigma-mw"));
		Duration tick = cfg.getDuration("meter-tick");
		startStream(tick);
		ActorMetrics.ensureSystem(profile(), ctx.getSystem().address().toString());
		ctx.getLog().info("MeterSimulator stream covering {} neighborhoods", neighborhoods.size());
	}

	private void startStream(Duration tick) {
		if (killSwitch != null) {
			killSwitch.shutdown();
		}
		ActorRef<Command> self = getContext().getSelf();
		String member = getContext().getSystem().address().toString();
		var pair = Source.tick(Duration.ZERO, tick, Boolean.TRUE)
				.viaMat(KillSwitches.single(), Keep.right())
				.map(u -> batch())
				.mapConcat(list -> list)
				.buffer(12, OverflowStrategy.dropHead())
				.toMat(Sink.foreach(n -> emit(n, member)), Keep.both())
				.run(materializer);
		this.killSwitch = pair.first();
		// dropHead has no callback — sample pressure via stress + dropWindow from batch shrink
		getContext().getSystem().scheduler().scheduleAtFixedRate(
				Duration.ofSeconds(2),
				Duration.ofSeconds(2),
				() -> {
					int d = dropWindow.getAndSet(0);
					if (d > 0 || stress.get()) {
						self.tell(new StreamDrop(Math.max(d, stress.get() ? 1 : 0)));
					}
				},
				getContext().getExecutionContext());
	}

	/** Build tell batch; under stress shrink (proxy for dropped work). */
	private List<Neighborhood> batch() {
		if (neighborhoods.isEmpty()) {
			return List.of();
		}
		int n = stress.get() ? 2 : 6;
		int want = stress.get() ? 6 : 6;
		if (stress.get()) {
			dropWindow.addAndGet(want - n);
		}
		List<Neighborhood> out = new ArrayList<>(n);
		for (int i = 0; i < n; i++) {
			int idx = Math.floorMod(cursor.getAndIncrement(), neighborhoods.size());
			out.add(neighborhoods.get(idx));
		}
		return out;
	}

	private void emit(Neighborhood n, String member) {
		double hour = currentHour();
		double scenario = stress.get() ? stressMultiplier : 1.0;
		double mw = loadModel.sampleMw(
				n.id(), n.districtId(), hour, scenario, ThreadLocalRandom.current().nextGaussian());
		sharding.entityRefFor(HouseActor.TYPE_KEY, n.id()).tell(new HouseProtocol.ConsumeWatts(mw));
		loadShare.tell(new LoadShare.LocalLoad(1));
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder()
				.onMessage(SetStress.class, this::onStress)
				.onMessage(SetHour.class, this::onHour)
				.onMessage(StreamDrop.class, this::onDrop)
				.build();
	}

	private Behavior<Command> onStress(SetStress msg) {
		stress.set(msg.enabled());
		getContext().getLog().info("Meter stress {}", msg.enabled());
		return this;
	}

	private Behavior<Command> onHour(SetHour msg) {
		simHour.set(msg.hour());
		getContext().getLog().info("Sim hour {}", msg.hour() < 0 ? "live" : msg.hour());
		return this;
	}

	private Behavior<Command> onDrop(StreamDrop msg) {
		uiHub.tell(new UiHub.Trace(ActorTrace.stream(
				"backpressure:" + msg.dropped(), getContext().getSystem().address().toString())));
		return this;
	}

	private double currentHour() {
		double h = simHour.get();
		if (h >= 0) {
			return h;
		}
		return (System.currentTimeMillis() / 3_600_000.0) % 24.0;
	}
}
