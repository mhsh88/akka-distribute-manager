package ir.sharifi.townpower;

/** Stable neighborhood / district id helpers. Format: d{district}-n{index} */
public final class TownIds {
	private TownIds() {
	}

	public static String neighborhoodId(int district, int index) {
		return "d" + district + "-n" + index;
	}

	public static int districtOf(String neighborhoodId) {
		int dash = neighborhoodId.indexOf('-');
		return Integer.parseInt(neighborhoodId.substring(1, dash));
	}

	public static int neighborhoodNoOf(String neighborhoodId) {
		int n = neighborhoodId.indexOf("-n");
		return Integer.parseInt(neighborhoodId.substring(n + 2));
	}
}
