package edu.sdsc.alignment;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.align.ce.CeCPMain;
import org.biojava.nbio.structure.align.ce.CeMain;
import org.biojava.nbio.structure.align.ce.CeSideChainMain;
import org.biojava.nbio.structure.align.fatcat.FatCatFlexible;
import org.biojava.nbio.structure.align.fatcat.FatCatRigid;
import org.biojava.nbio.structure.align.seq.SmithWaterman3Daligner;

import scala.Tuple2;

/**
 * Calculates the structural alignment between two chains.
 * 
 * @author Peter Rose
 *
 */
public class StructuralAlignmentMapper implements FlatMapFunction<Tuple2<Integer, Integer>, Row> {
	private static final long serialVersionUID = -3505435321631952639L;

	private String alignmentAlgorithm;
	private Broadcast<List<Tuple2<String, Point3d[]>>> chains = null;
	private List<String> biojavaAlgorithms = Arrays.asList(SmithWaterman3Daligner.algorithmName, CeMain.algorithmName,
			CeSideChainMain.algorithmName, CeCPMain.algorithmName, FatCatFlexible.algorithmName,
			FatCatRigid.algorithmName);

	public StructuralAlignmentMapper(Broadcast<List<Tuple2<String, Point3d[]>>> chains, String alignmentAlgorithm) {
		this.chains = chains;
		this.alignmentAlgorithm = alignmentAlgorithm;
	}

	@Override
	public Iterator<Row> call(Tuple2<Integer, Integer> pair) throws Exception {		
		int xIndex = pair._1();
		int yIndex = pair._2();
		
		// get list from broadcasted object
		List<Tuple2<String, Point3d[]>> data = this.chains.getValue();
		
		// get chain names
		String xId = data.get(xIndex)._1();
		String yId = data.get(yIndex)._1();
		
		// get coordinate arrays
		Point3d[] x = data.get(xIndex)._2();
		Point3d[] y = data.get(yIndex)._2();

		String key = xId + "-" + yId;
		
		List<Row> rows = Collections.emptyList();
		
		if (biojavaAlgorithms.contains(alignmentAlgorithm)) {
		    rows = BiojavaAligner.getScores(alignmentAlgorithm, key, x, y);
		    
		} else if (ExhaustiveAligner.alignmentAlgorithm.equals(alignmentAlgorithm)) {
			rows = ExhaustiveAligner.getScores(key, x, y);
		}

		return rows.iterator();
	}
}