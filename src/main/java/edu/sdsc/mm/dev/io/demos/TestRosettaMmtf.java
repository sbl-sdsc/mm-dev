package edu.sdsc.mm.dev.io.demos;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ContainsDProteinChain;
import edu.sdsc.mmtf.spark.filters.ContainsDnaChain;
import edu.sdsc.mmtf.spark.filters.ContainsGroup;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.io.MmtfImporter;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.MmtfWriter;
import edu.sdsc.mmtf.spark.io.demos.TraverseStructureHierarchy;

/**
 * Converts a directory containing PDB files into an MMTF-Hadoop Sequence file
 * with "full" (all atom, full precision) representation. The input directory 
 * is traversed recursively to find PDB files.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class TestRosettaMmtf {

    /**
     * Test: Read MMTF-Hadoop Sequence file.
     * 
     * @param args args[0] <path-to-mmtf-haddop-sequence-file>
     * 
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws FileNotFoundException {  

        // instantiate Spark
        //TODO set to local[1] !!!!
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("TestSwissModelMmtf");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long start = System.nanoTime();
        
        // read PDB files recursively starting the specified directory
        JavaPairRDD<String, StructureDataInterface> structures = MmtfReader.readSequenceFile(args[0], sc);

        // total:  639 structures
 //       structures = structures.filter(new ContainsDnaChain()); //  ?
 //      structures = structures.filter(new ContainsLProteinChain()); // 639?
 //       structures = structures.filter(new ContainsGroup("ZN")); // 0
 //       structures = structures.filter(new ContainsGroup("ATP")); //
        
        // debug: print structure data
 //       structures.foreach(t -> TraverseStructureHierarchy.demo(t._2));
        
 //       structures.foreach(t -> System.out.println(t._1));
        System.out.println(structures.map(t -> t._2.getNumEntities()).reduce((a, b) -> a+b));
        System.out.println("Number of structures read: " + structures.count());

        long end = System.nanoTime();
        System.out.println("Time: " + (end-start)/1E9 + " sec.");
        // close Spark
        sc.close(); 
    }
}
