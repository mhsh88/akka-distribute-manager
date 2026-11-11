package ir.sharifi.townpower;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;

import ir.sharifi.townpower.crdt.LoadShare;
import ir.sharifi.townpower.grid.GridController;
import ir.sharifi.townpower.grid.GridProtocol;
import ir.sharifi.townpower.house.HouseActor;
import ir.sharifi.townpower.house.HouseProtocol;
import ir.sharifi.townpower.http.UiHub;
import ir.sharifi.townpower.sim.MeterSimulator;

public final class Main {

	public static void main(String[] args) {
		int port = args.length > 0 ? Integer.parseInt(args[0]) : 2551;
		String hostname = env("PEKKO_HOSTNAME", "127.0.0.1");
		String bindHost = env("PEKKO_BIND_HOSTNAME", hostname);
		String seeds = env(
				"PEKKO_SEEDS",
				"pekko://town-power@127.0.0.1:2551,pekko://town-power@127.0.0.1:2552");

		StringBuilder seedHocon = new StringBuilder("pekko.cluster.seed-nodes = [");
		String[] parts = seeds.split(",");
		for (int i = 0; i < parts.length; i++) {
			if (i > 0) {
				seedHocon.append(", ");
			}
			seedHocon.append('"').append(parts[i].trim()).append('"');
		}
		seedHocon.append(']');

		Config config = ConfigFactory
				.parseString(
						"pekko.remote.artery.canonical.hostname=\"" + hostname + "\"\n"
								+ "pekko.remote.artery.canonical.port=" + port + "\n"
								+ "pekko.remote.artery.bind.hostname=\"" + bindHost + "\"\n"
								+ "pekko.remote.artery.bind.port=" + port + "\n"
								+ seedHocon)
				.withFallback(ConfigFactory.load());
		ActorSystem.create(Root.create(), "town-power", config);
	}

	private static String env(String key, String fallback) {
		String v = System.getenv(key);
		return (v == null || v.isBlank()) ? fallback : v;
	}

	private static final class Root {
		static Behavior<Void> create() {
			return Behaviors.setup(ctx -> {
				ActorRef<UiHub.Command> uiHub = ctx.spawn(UiHub.create(), "uiHub");
				ActorRef<GridProtocol.Command> grid = GridController.startSingleton(ctx, uiHub);

				ClusterSharding.get(ctx.getSystem()).init(
						Entity.of(
										HouseActor.TYPE_KEY,
										entityCtx -> HouseActor.create(entityCtx.getEntityId(), uiHub, grid))
								.withStopMessage(HouseProtocol.Passivate.INSTANCE));

				ActorRef<LoadShare.Command> loadShare = ctx.spawn(LoadShare.create(uiHub), "loadShare");
				ActorRef<MeterSimulator.Command> sim =
						ctx.spawn(MeterSimulator.create(uiHub, loadShare), "meterSim");
				uiHub.tell(new UiHub.Wire(grid, sim));

				ctx.getLog().info("town-power node ready at {}", ctx.getSystem().address());
				return Behaviors.empty();
			});
		}
	}

	private Main() {
	}
}
