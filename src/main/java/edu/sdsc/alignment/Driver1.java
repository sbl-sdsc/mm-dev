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
import org.biojava.nbio.structure.align.ce.CeCPMain;
import org.biojava.nbio.structure.align.ce.CeMain;
import org.biojava.nbio.structure.align.fatcat.FatCatFlexible;
import org.biojava.nbio.structure.align.fatcat.FatCatRigid;
import org.biojava.nbio.structure.align.seq.SmithWaterman3Daligner;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.rcsbfilters.Pisces;

public class Driver1 {

	public static void main(String[] args) throws IOException {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Driver1.class.getSimpleName());	
		JavaSparkContext sc = new JavaSparkContext(conf);

		long start = System.nanoTime();

		// download query structure
//		List<String> queryId = Arrays.asList("2O9U");
		List<String> queryId = Arrays.asList("1STP");
		JavaPairRDD<String, StructureDataInterface> query = MmtfReader.downloadMmtfFiles(queryId, false, true, sc)
				.flatMapToPair(new StructureToPolymerChains(false, true));
		
		// Examples similar: 4N6T, 2CH9, 3UL5, 3KVP
		// Examples dissimilar: 5O5I, 1STP, 
//		List<String> targetId = Arrays.asList("4N6T", "2CH9", "3UL5", "3KVP", "1STP", "5O5I");
		List<String> targetId = Arrays.asList("4OKA");
		JavaPairRDD<String, StructureDataInterface> target = MmtfReader.downloadMmtfFiles(targetId, false, true, sc)
				.flatMapToPair(new StructureToPolymerChains(false, true));
		
		// two standard algorithms
//		String alignmentAlgorithm = CeMain.algorithmName;
//		String alignmentAlgorithm = FatCatRigid.algorithmName;
		String alignmentAlgorithm = "exhaustive";
		
		// calculate alignments	
		Dataset<Row> alignments = StructureAligner.getQueryVsAllAlignments(query, target, alignmentAlgorithm).cache();
		
		alignments.coalesce(1).write().mode("overwrite").format("csv").save(args[0]);
		
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
