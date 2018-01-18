package edu.sdsc.alignment;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.vecmath.Point3d;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * This class performs exhaustive structural alignments between two protein chains.
 * It returns zero or more alternative alignments for each pair of chains based
 * on some evaluation criteria.
 * 
 * @author 
 *
 */
public class ExhaustiveAligner implements Serializable {
	private static final long serialVersionUID = -535570860330510219L;
	private int minLength;
	private int minCoverage;
	private double maxRmsd;
	private double minTm;

	public int getMinLength() {
		return minLength;
	}

	public void setMinLength(int minLength) {
		this.minLength = minLength;
	}

	public int getMinCoverage() {
		return minCoverage;
	}

	public void setMinCoverage(int minCoverage) {
		this.minCoverage = minCoverage;
	}

	public double getMaxRmsd() {
		return maxRmsd;
	}

	public void setMaxRmsd(double maxRmsd) {
		this.maxRmsd = maxRmsd;
	}

	public double getMinTm() {
		return minTm;
	}

	public void setMinTm(double minTm) {
		this.minTm = minTm;
	}

	// supported algorithms
	private static List<String> EXHAUSTIVE_ALGORITHMS = Arrays.asList(
			"exhaustive");
//			"exhaustive", "dynamicProgramming"); // ... more in the future
	/**
	 * Returns true if the alignment algorithm is supported by this class.
	 * @param alignmentAlgorithm name of the algorithm
	 * @return true, if algorithm is supported
	 */
	public static boolean isSupportedAlgorithm(String alignmentAlgorithm) {
		return EXHAUSTIVE_ALGORITHMS.contains(alignmentAlgorithm);
	}
	
	/**
	 * Returns one or more structure alignments and their alignment scores.
	 * 
	 * @param alignmentAlgorithm name of the algorithm
	 * @param key unique identifier for protein chain pair
	 * @param points1 C-alpha positions of chain 1
	 * @param points2 C-alpha positions of chain 2
	 * @return list of alignment metrics
	 */
	public List<Row> getAlignments(String alignmentAlgorithm, String key, Point3d[] points1, Point3d[] points2) {	
		List<Row> rows = new ArrayList<>();
		
		// TODO implement exhaustive alignments here ...
		
		// create some dummy data for now ...
		for (int i = 0; i < 3; i++) {
			int length = 100;
			int coverage1 = 100;
			int coverage2 = 100;
			float rmsd = 0.0f;
			float tm = 1.0f;
			
			int maxCoverage = Math.max(coverage1, coverage2);
			
			// store solutions that satisfy minimal criteria
			if (length > minLength && maxCoverage > minCoverage && rmsd < maxRmsd && tm > minTm) {
				// create a row of alignment metrics
				Row row = RowFactory.create(key, length, coverage1, coverage2, rmsd, tm);
				rows.add(row);
			}
		}

		return rows;
	}
}
