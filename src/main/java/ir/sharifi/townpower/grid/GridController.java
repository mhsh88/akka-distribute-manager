package ir.sharifi.townpower.grid;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.typed.ClusterSingleton;
import org.apache.pekko.cluster.typed.SingletonActor;

import ir.sharifi.townpower.ActorTrace;
import ir.sharifi.townpower.CriticalSites;
import ir.sharifi.townpower.NeighborhoodCatalog;
import ir.sharifi.townpower.NeighborhoodCatalog.Neighborhood;
import ir.sharifi.townpower.TownIds;
import ir.sharifi.townpower.grid.GridProtocol.Command;
import ir.sharifi.townpower.grid.GridProtocol.DelayedCriticalTrip;
import ir.sharifi.townpower.grid.GridProtocol.HouseTelemetry;
import ir.sharifi.townpower.grid.GridProtocol.SetStress;
import ir.sharifi.townpower.grid.GridProtocol.Tick;
import ir.sharifi.townpower.house.HouseActor;
import ir.sharifi.townpower.house.HouseProtocol;
import ir.sharifi.townpower.house.HouseProtocol.Status;
import ir.sharifi.townpower.http.UiHub;

/**
 * Cluster singleton supervisor: DSM shed of heavy محله kids, then cascade blackout if needed.
 * Shed children reconnect when district siblings are healthy (margin OK).
 */
public final class GridController extends AbstractBehavior<Command> {

	private static final Duration CRITICAL_HOLD = Duration.ofSeconds(6);

	private final ClusterSharding sharding;
	private final ActorRef<UiHub.Command> uiHub;
	private final NeighborhoodCatalog catalog;
	private final CriticalSites criticalSites;
	private final TimerScheduler<Command> timers;
	private final int districts;
	private final double capacityMw;
	private final Duration overloadStableFor;
	private final Duration blackoutCooldown;
	private final double cascadeBumpRatio;
	private final double shedRatio;
	private final double restoreRatio;
	private final int shedPerTick;

	private final Map<String, Double> loadByNeighborhood = new HashMap<>();
	private final Map<String, Status> statusByNeighborhood = new HashMap<>();
	private final Map<Integer, Instant> overloadSince = new HashMap<>();
	private final Map<Integer, Instant> blackoutAt = new HashMap<>();
	private final Set<Integer> blackedOut = new HashSet<>();
	private final Map<Integer, Double> cascadeBump = new HashMap<>();
	/** Supervisor-shed entity ids (Status.SHED from DSM, not cascade). */
	private final Set<String> supervisorShed = new HashSet<>();
	/** Districts that shed this tick — skip restore same tick (hysteresis). */
	private final Set<Integer> shedThisTick = new HashSet<>();
	private final Map<Integer, Instant> healthySince = new HashMap<>();
	/** Ask/reply ping: entityId → ask wall-clock ms. */
	private final Map<String, Long> askAtMs = new HashMap<>();
	private final Map<String, Integer> askAttempt = new HashMap<>();
	private final ActorRef<HouseProtocol.StatusView> statusAdapter;
	private int tickN;
	private boolean stress;

	private record StatusReply(HouseProtocol.StatusView view) implements Command {
	}

	/** At-least-once: retry GetStatus if no ack. */
	private record AskTimeout(String id, int attempt) implements Command {
	}

	public static ActorRef<Command> startSingleton(ActorContext<?> ctx, ActorRef<UiHub.Command> uiHub) {
		return ClusterSingleton.get(ctx.getSystem())
				.init(SingletonActor.of(create(uiHub), "GridController"));
	}

	public static Behavior<Command> create(ActorRef<UiHub.Command> uiHub) {
		return Behaviors.setup(ctx -> Behaviors.withTimers(timers -> new GridController(ctx, timers, uiHub)));
	}

	private GridController(ActorContext<Command> ctx, TimerScheduler<Command> timers, ActorRef<UiHub.Command> uiHub) {
		super(ctx);
		this.uiHub = uiHub;
		this.timers = timers;
		this.sharding = ClusterSharding.get(ctx.getSystem());
		this.catalog = NeighborhoodCatalog.get();
		this.criticalSites = CriticalSites.get();
		this.statusAdapter = ctx.messageAdapter(HouseProtocol.StatusView.class, StatusReply::new);
		var cfg = ctx.getSystem().settings().config().getConfig("town");
		this.districts = cfg.getInt("districts");
		this.capacityMw = cfg.getDouble("district-capacity-mw");
		this.overloadStableFor = cfg.getDuration("overload-stable-for");
		this.blackoutCooldown = cfg.getDuration("blackout-cooldown");
		this.cascadeBumpRatio = cfg.getDouble("cascade-bump-ratio");
		this.shedRatio = cfg.getDouble("supervisor-shed-ratio");
		this.restoreRatio = cfg.getDouble("supervisor-restore-ratio");
		this.shedPerTick = cfg.getInt("supervisor-shed-per-tick");
		timers.startTimerWithFixedDelay(Tick.INSTANCE, Duration.ofMillis(400));
		String self = ctx.getSystem().address().toString();
		uiHub.tell(new UiHub.Trace(ActorTrace.crown(self)));
		ctx.getLog().info(
				"GridController Tehran {} districts / {} neighborhoods / {} critical (supervisor DSM) crown={}",
				districts,
				catalog.all().size(),
				criticalSites.ids().size(),
				self);
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder()
				.onMessage(HouseTelemetry.class, this::onTelemetry)
				.onMessage(SetStress.class, this::onStress)
				.onMessage(DelayedCriticalTrip.class, this::onDelayedCritical)
				.onMessage(StatusReply.class, this::onStatusReply)
				.onMessage(AskTimeout.class, this::onAskTimeout)
				.onMessageEquals(Tick.INSTANCE, this::onTick)
				.build();
	}

	private Behavior<Command> onTelemetry(HouseTelemetry msg) {
		statusByNeighborhood.put(msg.houseId(), msg.status());
		if (msg.status() == Status.BLACKOUT || msg.status() == Status.SHED) {
			loadByNeighborhood.put(msg.houseId(), 0.0);
		} else {
			loadByNeighborhood.put(msg.houseId(), msg.watts());
			supervisorShed.remove(msg.houseId());
		}
		return this;
	}

	private Behavior<Command> onStress(SetStress msg) {
		if (stress == msg.enabled()) {
			return this;
		}
		stress = msg.enabled();
		getContext().getLog().info("Heatwave scenario {}", stress);
		if (!stress) {
			cascadeBump.clear();
			overloadSince.clear();
		}
		return this;
	}

	private Behavior<Command> onTick() {
		Instant now = Instant.now();
		double[] sums = districtSums();

		// 1) supervisor autonomy: shed heavy kids / restore when siblings healthy
		superviseDistricts(sums);

		sums = districtSums();

		// 2) cascade blackout only if still overloaded after DSM
		boolean trippedThisTick = false;
		for (int d = 1; d <= districts; d++) {
			double effective = sums[d] * (1.0 + cascadeBump.getOrDefault(d, 0.0));
			if (blackedOut.contains(d)) {
				overloadSince.remove(d);
				continue;
			}
			if (effective > capacityMw) {
				Instant since = overloadSince.computeIfAbsent(d, x -> now);
				if (!trippedThisTick && Duration.between(since, now).compareTo(overloadStableFor) >= 0) {
					trip(d, now);
					trippedThisTick = true;
				}
			} else {
				overloadSince.remove(d);
			}
		}

		decayCascadeBumps();
		maybeRestore(now, districtSums());
		if (++tickN % 5 == 0) {
			pingOneHouse();
		}
		uiHub.tell(new UiHub.GridSnapshot(districtSums(), Set.copyOf(blackedOut), capacityMw, stress));
		return this;
	}

	/** Actor Lab ask → ack with at-least-once retry on timeout. */
	private void pingOneHouse() {
		List<String> online = new ArrayList<>();
		for (Map.Entry<String, Status> e : statusByNeighborhood.entrySet()) {
			if (e.getValue() == Status.ONLINE && !askAtMs.containsKey(e.getKey())) {
				online.add(e.getKey());
			}
		}
		if (online.isEmpty()) {
			return;
		}
		String id = online.get(ThreadLocalRandom.current().nextInt(online.size()));
		askAtMs.put(id, System.currentTimeMillis());
		askAttempt.put(id, 1);
		sendAsk(id, 1);
	}

	private void sendAsk(String id, int attempt) {
		Neighborhood n = catalog.get(id);
		double lat = n == null ? 35.6892 : n.lat();
		double lng = n == null ? 51.3890 : n.lng();
		int districtId = n == null ? 0 : n.districtId();
		sharding.entityRefFor(HouseActor.TYPE_KEY, id).tell(new HouseProtocol.GetStatus(statusAdapter));
		String action = attempt <= 1 ? "ask" : "retry:" + attempt;
		uiHub.tell(new UiHub.Trace(ActorTrace.ping(
				action, id, districtId, lat, lng, getContext().getSystem().address().toString())));
		timers.startSingleTimer("ask:" + id, new AskTimeout(id, attempt), Duration.ofMillis(450));
	}

	private Behavior<Command> onAskTimeout(AskTimeout msg) {
		if (!askAtMs.containsKey(msg.id())) {
			return this; // already acked
		}
		Neighborhood n = catalog.get(msg.id());
		double lat = n == null ? 35.6892 : n.lat();
		double lng = n == null ? 51.3890 : n.lng();
		int districtId = n == null ? 0 : n.districtId();
		if (msg.attempt() >= 3) {
			askAtMs.remove(msg.id());
			askAttempt.remove(msg.id());
			uiHub.tell(new UiHub.Trace(ActorTrace.ping(
					"fail", msg.id(), districtId, lat, lng, getContext().getSystem().address().toString())));
			return this;
		}
		int next = msg.attempt() + 1;
		askAttempt.put(msg.id(), next);
		sendAsk(msg.id(), next);
		return this;
	}

	private Behavior<Command> onStatusReply(StatusReply msg) {
		HouseProtocol.StatusView v = msg.view();
		Long t0 = askAtMs.remove(v.neighborhoodId());
		askAttempt.remove(v.neighborhoodId());
		timers.cancel("ask:" + v.neighborhoodId());
		long ms = t0 == null ? -1 : Math.max(0, System.currentTimeMillis() - t0);
		Neighborhood n = catalog.get(v.neighborhoodId());
		double lat = n == null ? 35.6892 : n.lat();
		double lng = n == null ? 51.3890 : n.lng();
		uiHub.tell(new UiHub.Trace(ActorTrace.ping(
				"ack:" + ms,
				v.neighborhoodId(),
				v.districtId(),
				lat,
				lng,
				v.memberId() == null ? "" : v.memberId())));
		return this;
	}

	private double[] districtSums() {
		double[] sums = new double[districts + 1];
		for (var e : loadByNeighborhood.entrySet()) {
			Status st = statusByNeighborhood.getOrDefault(e.getKey(), Status.ONLINE);
			if (st == Status.BLACKOUT || st == Status.SHED) {
				continue;
			}
			int d = TownIds.districtOf(e.getKey());
			if (d >= 1 && d <= districts) {
				sums[d] += e.getValue();
			}
		}
		return sums;
	}

	/**
	 * Per منطقه: if load high → shed heaviest non-critical online kids.
	 * If margin healthy and shed kids waiting → restore while good siblings online.
	 */
	private void superviseDistricts(double[] sums) {
		Instant now = Instant.now();
		shedThisTick.clear();
		for (int d = 1; d <= districts; d++) {
			if (blackedOut.contains(d)) {
				healthySince.remove(d);
				continue;
			}
			double load = sums[d];
			if (load >= capacityMw * shedRatio) {
				healthySince.remove(d);
				shedHeaviest(d, shedPerTick);
				shedThisTick.add(d);
			} else if (load <= capacityMw * restoreRatio) {
				Instant since = healthySince.computeIfAbsent(d, x -> now);
				// hold healthy ~2s before reconnect (avoid shed/restore flap)
				if (!shedThisTick.contains(d)
						&& Duration.between(since, now).compareTo(Duration.ofSeconds(2)) >= 0) {
					restoreShedKids(d);
				}
			} else {
				healthySince.remove(d);
			}
		}
	}

	private void shedHeaviest(int district, int limit) {
		List<Map.Entry<String, Double>> heavy = new ArrayList<>();
		for (var e : loadByNeighborhood.entrySet()) {
			String id = e.getKey();
			if (TownIds.districtOf(id) != district) {
				continue;
			}
			if (criticalSites.isCritical(id)) {
				continue;
			}
			Status st = statusByNeighborhood.getOrDefault(id, Status.ONLINE);
			if (st != Status.ONLINE) {
				continue;
			}
			if (e.getValue() <= 0) {
				continue;
			}
			heavy.add(e);
		}
		heavy.sort(Comparator.comparingDouble((Map.Entry<String, Double> e) -> e.getValue()).reversed());
		int n = Math.min(limit, heavy.size());
		for (int i = 0; i < n; i++) {
			String id = heavy.get(i).getKey();
			double mw = heavy.get(i).getValue();
			sharding.entityRefFor(HouseActor.TYPE_KEY, id)
					.tell(new HouseProtocol.Blackout("supervisor-shed"));
			loadByNeighborhood.put(id, 0.0);
			statusByNeighborhood.put(id, Status.SHED);
			supervisorShed.add(id);
			getContext().getLog().info("SUPERVISOR SHED {} ({} MW) منطقه {}", id, String.format("%.1f", mw), district);
		}
	}

	/** Reconnect shed kids only while enough good (online) siblings remain. */
	private void restoreShedKids(int district) {
		List<String> waiting = new ArrayList<>();
		int goodOnline = 0;
		for (Neighborhood n : catalog.all()) {
			if (n.districtId() != district) {
				continue;
			}
			Status st = statusByNeighborhood.getOrDefault(n.id(), Status.ONLINE);
			if (st == Status.ONLINE) {
				goodOnline++;
			} else if (st == Status.SHED && supervisorShed.contains(n.id())) {
				waiting.add(n.id());
			}
		}
		if (waiting.isEmpty() || goodOnline < 2) {
			return; // need healthy siblings first
		}
		// restore lightest-first among shed (gentle reconnect)
		waiting.sort(Comparator.comparingDouble(id -> loadByNeighborhood.getOrDefault(id, 0.0)));
		int budget = Math.min(shedPerTick, waiting.size());
		for (int i = 0; i < budget; i++) {
			String id = waiting.get(i);
			sharding.entityRefFor(HouseActor.TYPE_KEY, id).tell(new HouseProtocol.Restore());
			statusByNeighborhood.put(id, Status.ONLINE);
			supervisorShed.remove(id);
			getContext().getLog().info("SUPERVISOR RESTORE {} منطقه {} (good siblings={})", id, district, goodOnline);
		}
	}

	private void trip(int district, Instant now) {
		blackedOut.add(district);
		blackoutAt.put(district, now);
		overloadSince.remove(district);
		getContext().getLog().warn("BLACKOUT منطقه {} (cascade)", district);

		boolean holdCritical = false;
		for (Neighborhood n : catalog.all()) {
			if (n.districtId() != district) {
				continue;
			}
			if (criticalSites.isCritical(n.id())) {
				holdCritical = true;
				continue;
			}
			sharding.entityRefFor(HouseActor.TYPE_KEY, n.id())
					.tell(new HouseProtocol.Blackout("منطقه-" + district + "-overload"));
			loadByNeighborhood.put(n.id(), 0.0);
			statusByNeighborhood.put(n.id(), Status.BLACKOUT);
			supervisorShed.remove(n.id());
		}

		if (holdCritical) {
			timers.startSingleTimer(
					"crit-" + district,
					new DelayedCriticalTrip(district),
					CRITICAL_HOLD);
			getContext().getLog().info("Critical sites in منطقه {} held for {}", district, CRITICAL_HOLD);
		}

		for (int n : catalog.neighborsOf(district)) {
			if (!blackedOut.contains(n)) {
				cascadeBump.merge(n, cascadeBumpRatio, Double::sum);
			}
		}
	}

	private Behavior<Command> onDelayedCritical(DelayedCriticalTrip msg) {
		int district = msg.districtId();
		if (!blackedOut.contains(district)) {
			return this;
		}
		for (Neighborhood n : catalog.all()) {
			if (n.districtId() != district || !criticalSites.isCritical(n.id())) {
				continue;
			}
			var site = criticalSites.get(n.id());
			sharding.entityRefFor(HouseActor.TYPE_KEY, n.id())
					.tell(new HouseProtocol.Blackout("critical-" + (site == null ? n.id() : site.displayName())));
			loadByNeighborhood.put(n.id(), 0.0);
			statusByNeighborhood.put(n.id(), Status.BLACKOUT);
			getContext().getLog().warn("CRITICAL DARK {} ({})", n.id(), site == null ? n.name() : site.displayName());
		}
		return this;
	}

	private void maybeRestore(Instant now, double[] sums) {
		if (stress) {
			double global = 0;
			for (int d = 1; d <= districts; d++) {
				global += sums[d];
			}
			double globalCap = capacityMw * districts;
			if (global > globalCap * 0.55) {
				return;
			}
		}
		Duration wait = stress ? blackoutCooldown : blackoutCooldown.dividedBy(2);
		Integer oldest = null;
		Instant oldestAt = null;
		for (int d : blackedOut) {
			Instant at = blackoutAt.get(d);
			if (at == null) {
				continue;
			}
			if (Duration.between(at, now).compareTo(wait) < 0) {
				continue;
			}
			if (oldestAt == null || at.isBefore(oldestAt)) {
				oldestAt = at;
				oldest = d;
			}
		}
		if (oldest != null) {
			restore(oldest);
		}
	}

	private void restore(int district) {
		timers.cancel("crit-" + district);
		blackedOut.remove(district);
		blackoutAt.remove(district);
		cascadeBump.remove(district);
		getContext().getLog().info("RESTORE منطقه {} (critical first)", district);

		for (Neighborhood n : catalog.all()) {
			if (n.districtId() != district || !criticalSites.isCritical(n.id())) {
				continue;
			}
			sharding.entityRefFor(HouseActor.TYPE_KEY, n.id()).tell(new HouseProtocol.Restore());
			statusByNeighborhood.put(n.id(), Status.ONLINE);
			supervisorShed.remove(n.id());
		}
		for (Neighborhood n : catalog.all()) {
			if (n.districtId() != district || criticalSites.isCritical(n.id())) {
				continue;
			}
			sharding.entityRefFor(HouseActor.TYPE_KEY, n.id()).tell(new HouseProtocol.Restore());
			statusByNeighborhood.put(n.id(), Status.ONLINE);
			supervisorShed.remove(n.id());
		}
	}

	private void decayCascadeBumps() {
		cascadeBump.replaceAll((d, v) -> Math.max(0, v * 0.92));
		cascadeBump.entrySet().removeIf(e -> e.getValue() < 0.01);
	}
}
