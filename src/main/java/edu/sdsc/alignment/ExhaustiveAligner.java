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
	public static List<Row> getAlignments(String alignmentAlgorithm, String key, Point3d[] points1, Point3d[] points2) {	
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
			if (length > 40 && maxCoverage > 50 && rmsd < 4.0 && tm > 0.4) {
				// create a row of alignment metrics
				Row row = RowFactory.create(key, length, coverage1, coverage2, rmsd, tm);
				rows.add(row);
			}
		}

		return rows;
	}
}
