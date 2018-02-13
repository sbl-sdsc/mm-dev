package edu.sdsc.mm.dev.io.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ContainsDnaChain;
import edu.sdsc.mmtf.spark.filters.ContainsGroup;
import edu.sdsc.mmtf.spark.io.MmtfImporter;
import edu.sdsc.mmtf.spark.io.MmtfWriter;
import edu.sdsc.mmtf.spark.io.demos.TraverseStructureHierarchy;

/**
 * Converts a SWISS-MODEL repository into an MMTF-Hadoop Sequence file.
 * 
 * <p>SWISS-PROT repositories for specific species can be downloaded from
 * the <a href="https://swissmodel.expasy.org/repository">SWISS-MODEL Repository</a>
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class SwissModelToMmtf {

    /**
     * Converts a SWISS-MODEL repository into an MMTF-Hadoop Sequence file.
     * 
     * @param args args[0] <path-to-repository>, args[1] <path-to-mmtf-hadoop-file>
     * 
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws FileNotFoundException {  

        if (args.length != 2) {
            System.out.println("Usage: SwissModelToMmtf <path-to-repository> <path-to-mmtf-hadoop-file>");
        }

        long start = System.nanoTime();
        // path to input directory
        String repositoryPath = args[0];

        // path to output directory
        String mmtfPath = args[1];

        // instantiate Spark
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SwissModelToMmtf");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read PDB files recursively starting the specified directory
        JavaPairRDD<String, StructureDataInterface> structures = MmtfImporter.importSwissModelRepository(repositoryPath, sc);

 //       System.out.println("structures: " + structures.count());
 //       structures.foreach(t -> System.out.println(t._1 + ":" + t._2.getNumEntities()));
 //       structures = structures.filter(new ContainsDnaChain());
        
 //       System.out.println("# structures: " + structures.count());
        // debug: print structure data
 //       structures.foreach(t -> TraverseStructureHierarchy.demo(t._2));
        
        // save as an MMTF-Hadoop Sequence File
        MmtfWriter.writeSequenceFile(mmtfPath, sc, structures);

        System.out.println("Time: " + (System.nanoTime()-start)/1E9 + " sec.");
        
        // close Spark
        sc.close(); 
    }
}
