package ir.sharifi.townpower.http;

import java.util.Set;

import org.apache.pekko.actor.DeadLetter;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.receptionist.Receptionist;
import org.apache.pekko.actor.typed.receptionist.ServiceKey;
import org.apache.pekko.cluster.ClusterEvent;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.cluster.typed.Subscribe;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import ir.sharifi.townpower.ActorTrace;
import ir.sharifi.townpower.CborSerializable;
import ir.sharifi.townpower.grid.GridProtocol;
import ir.sharifi.townpower.house.HouseActor;
import ir.sharifi.townpower.house.HouseProtocol;
import ir.sharifi.townpower.house.HouseProtocol.HouseEvent;
import ir.sharifi.townpower.sim.MeterSimulator;

/** Local UI gateway: house/grid events, receptionist fan-out, HTTP snapshots. */
public final class UiHub extends AbstractBehavior<UiHub.Command> {

	public static final ServiceKey<Command> SERVICE_KEY = ServiceKey.create(Command.class, "UiHub");

	public interface Command extends CborSerializable {
	}

	public record HouseChanged(HouseEvent event) implements Command {
		@JsonCreator
		public HouseChanged(@JsonProperty("event") HouseEvent event) {
			this.event = event;
		}
	}

	public record RemoteHouse(HouseEvent event) implements Command {
		@JsonCreator
		public RemoteHouse(@JsonProperty("event") HouseEvent event) {
			this.event = event;
		}
	}

	public record GridSnapshot(double[] districtSums, Set<Integer> blackouts, double capacity, boolean stress)
			implements Command {
		@JsonCreator
		public GridSnapshot(
				@JsonProperty("districtSums") double[] districtSums,
				@JsonProperty("blackouts") Set<Integer> blackouts,
				@JsonProperty("capacity") double capacity,
				@JsonProperty("stress") boolean stress) {
			this.districtSums = districtSums;
			this.blackouts = blackouts;
			this.capacity = capacity;
			this.stress = stress;
		}
	}

	public record StressChanged(boolean enabled) implements Command {
		@JsonCreator
		public StressChanged(@JsonProperty("enabled") boolean enabled) {
			this.enabled = enabled;
		}
	}

	public record SetStress(boolean enabled) implements Command {
		@JsonCreator
		public SetStress(@JsonProperty("enabled") boolean enabled) {
			this.enabled = enabled;
		}
	}

	/** hour &lt; 0 means live wall clock */
	public record SetHour(double hour) implements Command {
		@JsonCreator
		public SetHour(@JsonProperty("hour") double hour) {
			this.hour = hour;
		}
	}

	public record HourChanged(double hour) implements Command {
		@JsonCreator
		public HourChanged(@JsonProperty("hour") double hour) {
			this.hour = hour;
		}
	}

	public record ShedNeighborhood(String id) implements Command {
		@JsonCreator
		public ShedNeighborhood(@JsonProperty("id") String id) {
			this.id = id;
		}
	}

	public record RestoreNeighborhood(String id) implements Command {
		@JsonCreator
		public RestoreNeighborhood(@JsonProperty("id") String id) {
			this.id = id;
		}
	}

	public record Wire(ActorRef<GridProtocol.Command> grid, ActorRef<MeterSimulator.Command> simulator)
			implements Command {
	}

	public record Trace(ActorTrace event) implements Command {
		@JsonCreator
		public Trace(@JsonProperty("event") ActorTrace event) {
			this.event = event;
		}
	}

	public record RemoteTrace(ActorTrace event) implements Command {
		@JsonCreator
		public RemoteTrace(@JsonProperty("event") ActorTrace event) {
			this.event = event;
		}
	}

	/** Force a few DeadLetters for Actor Lab siren demo. */
	public enum FireDeadDemo implements Command {
		INSTANCE
	}

	private record ListingAdapter(Receptionist.Listing listing) implements Command {
	}

	private record MemberAdapter(ClusterEvent.MemberEvent event) implements Command {
	}

	private record ReachAdapter(ClusterEvent.ReachabilityEvent event) implements Command {
	}

	private final HttpGateway http;
	private ActorRef<GridProtocol.Command> grid;
	private ActorRef<MeterSimulator.Command> simulator;
	private Set<ActorRef<Command>> peers = Set.of();

	public static Behavior<Command> create() {
		return Behaviors.setup(UiHub::new);
	}

	private UiHub(ActorContext<Command> ctx) {
		super(ctx);
		ctx.getSystem().receptionist().tell(Receptionist.register(SERVICE_KEY, ctx.getSelf()));
		ActorRef<Receptionist.Listing> listingAdapter =
				ctx.messageAdapter(Receptionist.Listing.class, ListingAdapter::new);
		ctx.getSystem().receptionist().tell(Receptionist.subscribe(SERVICE_KEY, listingAdapter));
		ActorRef<ClusterEvent.MemberEvent> memberAdapter =
				ctx.messageAdapter(ClusterEvent.MemberEvent.class, MemberAdapter::new);
		ActorRef<ClusterEvent.ReachabilityEvent> reachAdapter =
				ctx.messageAdapter(ClusterEvent.ReachabilityEvent.class, ReachAdapter::new);
		Cluster cluster = Cluster.get(ctx.getSystem());
		cluster.subscriptions().tell(Subscribe.create(memberAdapter, ClusterEvent.MemberEvent.class));
		cluster.subscriptions().tell(Subscribe.create(reachAdapter, ClusterEvent.ReachabilityEvent.class));
		ActorRef<Command> self = ctx.getSelf();
		ctx.spawn(Behaviors.<DeadLetter>setup(c -> {
			Adapter.toClassic(c.getSystem()).eventStream()
					.subscribe(Adapter.toClassic(c.getSelf()), DeadLetter.class);
			return Behaviors.receive(DeadLetter.class)
					.onMessage(DeadLetter.class, dl -> {
						String type = dl.message() == null ? "?" : dl.message().getClass().getSimpleName();
						String recip = dl.recipient() == null ? "?" : dl.recipient().path().name();
						self.tell(new Trace(ActorTrace.dead(type, recip)));
						return Behaviors.same();
					})
					.build();
		}), "deadLetterWatch");
		this.http = HttpGateway.start(ctx.getSystem(), ctx.getSelf());
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder()
				.onMessage(Wire.class, this::onWire)
				.onMessage(HouseChanged.class, this::onHouse)
				.onMessage(RemoteHouse.class, this::onRemoteHouse)
				.onMessage(GridSnapshot.class, this::onGrid)
				.onMessage(StressChanged.class, this::onStressChanged)
				.onMessage(SetStress.class, this::onSetStress)
				.onMessage(SetHour.class, this::onSetHour)
				.onMessage(HourChanged.class, this::onHourChanged)
				.onMessage(ShedNeighborhood.class, this::onShed)
				.onMessage(RestoreNeighborhood.class, this::onRestore)
				.onMessage(Trace.class, this::onTrace)
				.onMessage(RemoteTrace.class, this::onRemoteTrace)
				.onMessageEquals(FireDeadDemo.INSTANCE, this::onFireDead)
				.onMessage(ListingAdapter.class, this::onListing)
				.onMessage(MemberAdapter.class, this::onMember)
				.onMessage(ReachAdapter.class, this::onReach)
				.build();
	}

	private Behavior<Command> onFireDead() {
		// Hit missing actor paths → DeadLetter → lab siren
		var classic = Adapter.toClassic(getContext().getSystem());
		for (int i = 0; i < 5; i++) {
			classic.actorSelection("/user/lab-dead-" + i).tell("bomb-" + i, Adapter.toClassic(getContext().getSelf()));
		}
		return this;
	}

	private Behavior<Command> onMember(MemberAdapter msg) {
		ClusterEvent.MemberEvent e = msg.event();
		// Local lab only — every node sees same membership; no receptionist fan-out.
		if (e instanceof ClusterEvent.MemberUp up) {
			http.onActorTrace(ActorTrace.watch("join", up.member().address().toString()));
		} else if (e instanceof ClusterEvent.MemberRemoved removed) {
			http.onActorTrace(ActorTrace.watch("leave", removed.member().address().toString()));
		}
		return this;
	}

	private Behavior<Command> onReach(ReachAdapter msg) {
		ClusterEvent.ReachabilityEvent e = msg.event();
		if (e instanceof ClusterEvent.UnreachableMember unreachable) {
			http.onActorTrace(ActorTrace.watch("unreachable", unreachable.member().address().toString()));
		} else if (e instanceof ClusterEvent.ReachableMember reachable) {
			http.onActorTrace(ActorTrace.watch("reachable", reachable.member().address().toString()));
		}
		return this;
	}

	private Behavior<Command> onWire(Wire msg) {
		this.grid = msg.grid();
		this.simulator = msg.simulator();
		return this;
	}

	private Behavior<Command> onListing(ListingAdapter msg) {
		peers = msg.listing().getServiceInstances(SERVICE_KEY);
		return this;
	}

	private Behavior<Command> onHouse(HouseChanged msg) {
		http.onHouseEvent(msg.event());
		for (ActorRef<Command> peer : peers) {
			if (!peer.equals(getContext().getSelf())) {
				peer.tell(new RemoteHouse(msg.event()));
			}
		}
		return this;
	}

	private Behavior<Command> onRemoteHouse(RemoteHouse msg) {
		http.onHouseEvent(msg.event());
		return this;
	}

	private Behavior<Command> onGrid(GridSnapshot msg) {
		http.onGridSnapshot(msg.districtSums(), msg.blackouts(), msg.capacity(), msg.stress());
		return this;
	}

	private Behavior<Command> onSetStress(SetStress msg) {
		if (grid != null) {
			grid.tell(new GridProtocol.SetStress(msg.enabled()));
		}
		applySimStress(msg.enabled());
		for (ActorRef<Command> peer : peers) {
			if (!peer.equals(getContext().getSelf())) {
				peer.tell(new StressChanged(msg.enabled()));
			}
		}
		return this;
	}

	private Behavior<Command> onStressChanged(StressChanged msg) {
		applySimStress(msg.enabled());
		return this;
	}

	private void applySimStress(boolean enabled) {
		if (simulator != null) {
			simulator.tell(new MeterSimulator.SetStress(enabled));
		}
		http.onStress(enabled);
	}

	private Behavior<Command> onSetHour(SetHour msg) {
		applyHour(msg.hour());
		for (ActorRef<Command> peer : peers) {
			if (!peer.equals(getContext().getSelf())) {
				peer.tell(new HourChanged(msg.hour()));
			}
		}
		return this;
	}

	private Behavior<Command> onHourChanged(HourChanged msg) {
		applyHour(msg.hour());
		return this;
	}

	private void applyHour(double hour) {
		if (simulator != null) {
			simulator.tell(new MeterSimulator.SetHour(hour));
		}
		http.onHour(hour);
	}

	private Behavior<Command> onTrace(Trace msg) {
		http.onActorTrace(msg.event());
		for (ActorRef<Command> peer : peers) {
			if (!peer.equals(getContext().getSelf())) {
				peer.tell(new RemoteTrace(msg.event()));
			}
		}
		return this;
	}

	private Behavior<Command> onRemoteTrace(RemoteTrace msg) {
		http.onActorTrace(msg.event());
		return this;
	}

	private Behavior<Command> onShed(ShedNeighborhood msg) {
		ClusterSharding.get(getContext().getSystem())
				.entityRefFor(HouseActor.TYPE_KEY, msg.id())
				.tell(new HouseProtocol.Blackout("manual-shed"));
		return this;
	}

	private Behavior<Command> onRestore(RestoreNeighborhood msg) {
		ClusterSharding.get(getContext().getSystem())
				.entityRefFor(HouseActor.TYPE_KEY, msg.id())
				.tell(new HouseProtocol.Restore());
		return this;
	}
}
