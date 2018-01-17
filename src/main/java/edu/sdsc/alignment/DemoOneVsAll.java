package edu.sdsc.alignment;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.align.fatcat.FatCatRigid;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.rcsbfilters.Pisces;

public class DemoOneVsAll {

	public static void main(String[] args) throws IOException {

		String path = System.getProperty("MMTF_REDUCED");
		if (path == null) {
			System.err.println("Environment variable for Hadoop sequence file has not been set");
			System.exit(-1);
		}

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(DemoOneVsAll.class.getSimpleName());	
		JavaSparkContext sc = new JavaSparkContext(conf);

		long start = System.nanoTime();

		// download query structure
		List<String> queryId = Arrays.asList("2W47");
		JavaPairRDD<String, StructureDataInterface> query = MmtfReader.downloadMmtfFiles(queryId, false, true, sc)
				.flatMapToPair(new StructureToPolymerChains());
		
		// use a 1 % sample of the PDB and then filter by the Pisces non-redundant set
		// at 20% sequence identity and a resolution better than 1.6 A.
		double fraction = 0.01;
		int seed = 123;
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, fraction, seed, sc)
				.flatMapToPair(new StructureToPolymerChains())
				.filter(new Pisces(20, 1.6));
		
		// specialized algorithms
//		String alignmentAlgorithm = CeMain.algorithmName;
//		String alignmentAlgorithm = CeCPMain.algorithmName;		
//		String alignmentAlgorithm = FatCatFlexible.algorithmName;
		
		// two main algorithm
//		String alignmentAlgorithm = CeMain.algorithmName;
		String alignmentAlgorithm = FatCatRigid.algorithmName;
//		String alignmentAlgorithm = ExhaustiveAligner.alignmentAlgorithm;
		
		// calculate alignments	
		Dataset<Row> alignments = StructureAligner.getOneVsAllAlignments(pdb, query, alignmentAlgorithm).cache();
		
		// show results
	    int count = (int)alignments.count();	
		alignments.sort(col("tm").desc()).show(count);		
        System.out.println("Pairs: " + count);
		
		long end = System.nanoTime();		
		System.out.println("Time per alignment: " + TimeUnit.NANOSECONDS.toMillis((end-start)/count) + " msec.");		
		System.out.println("Time: " + TimeUnit.NANOSECONDS.toSeconds(end-start) + " sec.");
		
		sc.close();
	}
}
