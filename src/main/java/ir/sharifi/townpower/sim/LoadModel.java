package ir.sharifi.townpower.sim;

/**
 * Per-محله MW load model.
 *
 * <pre>
 *   P_i(h) = P_base,i · f_diurnal(h) · f_temp(T(h)) · m_scenario + ε
 * </pre>
 *
 * Units MW. Stable P_base so supervisor can rank heavy children.
 */
public final class LoadModel {

	private final double baseMw;
	private final double northBoostMw;
	private final double tempRefC;
	private final double tempAlpha;
	private final double noiseSigmaMw;

	public LoadModel(double baseMw, double northBoostMw, double tempRefC, double tempAlpha, double noiseSigmaMw) {
		this.baseMw = baseMw;
		this.northBoostMw = northBoostMw;
		this.tempRefC = tempRefC;
		this.tempAlpha = tempAlpha;
		this.noiseSigmaMw = noiseSigmaMw;
		assert baseMw > 0 && tempAlpha >= 0 && noiseSigmaMw >= 0;
	}

	/** Diurnal shape: night trough, afternoon/evening peak. Range ~0.45–1.05. */
	public double diurnalFactor(double hour) {
		double h = ((hour % 24) + 24) % 24;
		double wave = 0.55 + 0.45 * Math.sin((h - 8.0) / 24.0 * Math.PI * 2.0);
		if (h >= 13 && h <= 17) {
			wave = Math.max(wave, 0.92);
		}
		if (h >= 0 && h < 5) {
			wave = Math.min(wave, 0.48);
		}
		return wave;
	}

	/** Simple outdoor temp proxy from hour (°C). */
	public double ambientTempC(double hour) {
		double h = ((hour % 24) + 24) % 24;
		// cool night ~18, hot afternoon ~38
		return 28.0 + 10.0 * Math.sin((h - 10.0) / 24.0 * Math.PI * 2.0);
	}

	/** Cooling degree-day style multiplier. */
	public double tempFactor(double tempC) {
		return 1.0 + tempAlpha * Math.max(0.0, tempC - tempRefC);
	}

	/**
	 * Deterministic base for محله: district bias + id hash so heavy kids stay heavy.
	 */
	public double baseLoadMw(String neighborhoodId, int districtId) {
		double north = districtId <= 6 ? northBoostMw : 0.0;
		double districtBias = (districtId % 5) * 0.8;
		double idBias = (Math.floorMod(neighborhoodId.hashCode(), 1000) / 1000.0) * 4.0;
		return baseMw + north + districtBias + idBias;
	}

	public double sampleMw(String neighborhoodId, int districtId, double hour, double scenarioMultiplier, double gaussianNoise) {
		double pBase = baseLoadMw(neighborhoodId, districtId);
		double fDay = diurnalFactor(hour);
		double fTemp = tempFactor(ambientTempC(hour));
		double noise = gaussianNoise * noiseSigmaMw;
		return Math.max(1.0, pBase * fDay * fTemp * scenarioMultiplier + noise);
	}
}
