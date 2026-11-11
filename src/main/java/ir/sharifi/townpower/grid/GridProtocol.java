package ir.sharifi.townpower.grid;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import ir.sharifi.townpower.CborSerializable;
import ir.sharifi.townpower.house.HouseProtocol.Status;

public final class GridProtocol {
	private GridProtocol() {
	}

	public interface Command extends CborSerializable {
	}

	public record HouseTelemetry(
			String houseId,
			int districtId,
			double watts,
			Status status) implements Command {
		@JsonCreator
		public HouseTelemetry(
				@JsonProperty("houseId") String houseId,
				@JsonProperty("districtId") int districtId,
				@JsonProperty("watts") double watts,
				@JsonProperty("status") Status status) {
			this.houseId = houseId;
			this.districtId = districtId;
			this.watts = watts;
			this.status = status;
		}
	}

	/** Demo knob: multiply simulated load until cascade. */
	public record SetStress(boolean enabled) implements Command {
		@JsonCreator
		public SetStress(@JsonProperty("enabled") boolean enabled) {
			this.enabled = enabled;
		}
	}

	public record StressState(boolean enabled) implements CborSerializable {
		@JsonCreator
		public StressState(@JsonProperty("enabled") boolean enabled) {
			this.enabled = enabled;
		}
	}

	public enum Tick implements Command {
		INSTANCE
	}

	/** Second-wave blackout for hospitals / metro / stadium in a district. */
	public record DelayedCriticalTrip(int districtId) implements Command {
		@JsonCreator
		public DelayedCriticalTrip(@JsonProperty("districtId") int districtId) {
			this.districtId = districtId;
		}
	}
}
