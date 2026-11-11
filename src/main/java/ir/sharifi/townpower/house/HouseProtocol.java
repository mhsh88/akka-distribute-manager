package ir.sharifi.townpower.house;

import org.apache.pekko.actor.typed.ActorRef;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import ir.sharifi.townpower.CborSerializable;

public final class HouseProtocol {
	private HouseProtocol() {
	}

	public interface Command extends CborSerializable {
	}

	public enum Status {
		ONLINE, BLACKOUT, SHED
	}

	public record ConsumeWatts(double watts) implements Command {
		@JsonCreator
		public ConsumeWatts(@JsonProperty("watts") double watts) {
			this.watts = watts;
		}
	}

	public record Blackout(String reason) implements Command {
		@JsonCreator
		public Blackout(@JsonProperty("reason") String reason) {
			this.reason = reason;
		}
	}

	public record Restore() implements Command {
	}

	public record GetStatus(ActorRef<StatusView> replyTo) implements Command {
		@JsonCreator
		public GetStatus(@JsonProperty("replyTo") ActorRef<StatusView> replyTo) {
			this.replyTo = replyTo;
		}
	}

	public enum Passivate implements Command {
		INSTANCE
	}

	public record StatusView(
			String neighborhoodId,
			String name,
			int districtId,
			double watts,
			Status status,
			String memberId,
			String shardId) implements CborSerializable {
		@JsonCreator
		public StatusView(
				@JsonProperty("neighborhoodId") String neighborhoodId,
				@JsonProperty("name") String name,
				@JsonProperty("districtId") int districtId,
				@JsonProperty("watts") double watts,
				@JsonProperty("status") Status status,
				@JsonProperty("memberId") String memberId,
				@JsonProperty("shardId") String shardId) {
			this.neighborhoodId = neighborhoodId;
			this.name = name;
			this.districtId = districtId;
			this.watts = watts;
			this.status = status;
			this.memberId = memberId;
			this.shardId = shardId;
		}
	}

	/** Fan-out to UI hubs and grid. Entity = Tehran محله. */
	public record HouseEvent(
			String neighborhoodId,
			String name,
			int districtId,
			double lat,
			double lng,
			double watts,
			Status status,
			String action,
			String memberId,
			String shardId) implements CborSerializable {
		@JsonCreator
		public HouseEvent(
				@JsonProperty("neighborhoodId") String neighborhoodId,
				@JsonProperty("name") String name,
				@JsonProperty("districtId") int districtId,
				@JsonProperty("lat") double lat,
				@JsonProperty("lng") double lng,
				@JsonProperty("watts") double watts,
				@JsonProperty("status") Status status,
				@JsonProperty("action") String action,
				@JsonProperty("memberId") String memberId,
				@JsonProperty("shardId") String shardId) {
			this.neighborhoodId = neighborhoodId;
			this.name = name;
			this.districtId = districtId;
			this.lat = lat;
			this.lng = lng;
			this.watts = watts;
			this.status = status;
			this.action = action;
			this.memberId = memberId;
			this.shardId = shardId;
		}
	}
}
