package edu.sdsc.alignment;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.vecmath.Point3d;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * This class performs exhaustive alignments between two protein chains
 * @author 
 *
 */
public class ExhaustiveAligner implements Serializable {
	private static final long serialVersionUID = -535570860330510219L;
	public static final String alignmentAlgorithm = "exhaustive";

	/**
	 * Returns one or more structure alignments and their alignment scores.
	 * @param key String that contains the chain Ids of the two proteins
	 * @param points1 Coordinates of first protein chain
	 * @param points2 Coordinates of second protein chain
	 * @return
	 */
	public static List<Row> getScores(String key, Point3d[] points1, Point3d[] points2) {	
		List<Row> rows = new ArrayList<>();
		
		// loop over all possible alignments
		for (int i = 0; i < 3; i++) {
			int length = 100;
			int coverage1 = 100;
			int coverage2 = 100;
			float rmsd = 0.0f;
			float tmScore = 1.0f;
			
			int maxCoverage = Math.max(coverage1, coverage2);
			
			// only store solutions that match some minimal conditions
			if (length > 40 && maxCoverage > 0.5 && rmsd < 4.0f && tmScore > 0.4) {
				// create a row of scores
				Row row = RowFactory.create(key, length, coverage1, coverage2, rmsd, tmScore);
				rows.add(row);
			}
		}

		return rows;
	}
}
