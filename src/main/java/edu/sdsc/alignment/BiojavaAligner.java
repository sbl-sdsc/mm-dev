package edu.sdsc.alignment;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import javax.vecmath.Point3d;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.biojava.nbio.structure.AminoAcidImpl;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.AtomImpl;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.ChainImpl;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.StructureException;
import org.biojava.nbio.structure.align.StructureAlignment;
import org.biojava.nbio.structure.align.StructureAlignmentFactory;
import org.biojava.nbio.structure.align.model.AFPChain;
import org.biojava.nbio.structure.align.util.AFPChainScorer;

public class BiojavaAligner implements Serializable {
	private static final long serialVersionUID = 5027430987584019776L;
	private static final String CA_NAME = "CA";
	private static final String GROUP_NAME = "GLU";

	public static List<Row> getScores(String alignmentAlgorithm, String key, Point3d[] points1, Point3d[] points2) {	
		Atom[] ca1 = getCAAtoms(points1);
		Atom[] ca2 = getCAAtoms(points2);
		
		AFPChain afp = null;
		try {
			StructureAlignment algorithm  = StructureAlignmentFactory.getAlgorithm(alignmentAlgorithm);
			afp = algorithm.align(ca1,ca2);
			double tmScore = AFPChainScorer.getTMScore(afp, ca1, ca2);
			afp.setTMScore(tmScore);
		} catch (StructureException e) {
			e.printStackTrace();
			return Collections.emptyList();
		} 
		
		// TODO add alignments as arrays to results
//		int[][] alignment = afp.getAfpIndex();
//		for (int i = 0; i < alignment.length; i++) {
//			System.out.println(alignment[i][0] + " - " + alignment[i][1]);
//		}

		Row row = RowFactory.create(key, afp.getOptLength(), afp.getCoverage1(), 
				afp.getCoverage2(), (float) afp.getTotalRmsdOpt(), (float) afp.getTMScore());

		return Collections.singletonList(row);
	}

	private static Atom[] getCAAtoms(Point3d[] points) {
		int gaps = 0;
		for (Point3d p: points) {
			if (p == null) {
				gaps++;
			}
		}
		Chain c = new ChainImpl();
		c.setId("A");	

		Atom[] atoms = new Atom[points.length-gaps];

		for (int i = 0, j = 0; i < points.length; i++) {
			if (points[i] != null) {
				atoms[j] = new AtomImpl();
				atoms[j].setName(CA_NAME);
				Group g = new AminoAcidImpl();
				g.setPDBName(GROUP_NAME);
				g.setResidueNumber("A", i, ' ');
				g.addAtom(atoms[j]);
				c.addGroup(g);

				atoms[j].setX(points[i].x);
				atoms[j].setY(points[i].y);
				atoms[j].setZ(points[i].z);
				j++;
			}
		}

		return atoms;
	}
}
