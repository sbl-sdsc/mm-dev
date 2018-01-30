package edu.sdsc.mmtf.spark.incubator;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;

public class FileTest {

	public static void main(String[] args) throws IOException {
		
		String path = MmtfReader.getMmtfReducedPath();
	
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FileTest.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	  
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);
	    
	    pdb.foreach(t -> System.out.println(t._1 + ": " + t._2.getStructureId()));
	}
}
