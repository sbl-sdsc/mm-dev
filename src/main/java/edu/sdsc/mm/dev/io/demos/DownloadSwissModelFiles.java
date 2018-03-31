package edu.sdsc.mm.dev.io.demos;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfImporter;
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
public class DownloadSwissModelFiles {

    /**
     * Converts a directory containing Rosetta-style PDB files into an MMTF-Hadoop Sequence file.
     * The input directory is traversed recursively to find PDB files.
     * 
     * <p> Example files from Gremlin website:
     * https://gremlin2.bakerlab.org/meta/aah4043_final.zip
     * 
     * @param args args[0] <path-to-pdb_files>, args[1] <path-to-mmtf-hadoop-file>
     * 
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws FileNotFoundException {  

        // instantiate Spark
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("DownloadSwissProtModelFiles");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> uniProtIds = Arrays.asList("P22629", "Q9H2C2", "Q8WXK3");
//        List<String> uniProtIds = Arrays.asList("P07900");
        // read PDB files recursively starting the specified directory
        
        // TODO: Empty structure record for Q8WXK3
        JavaPairRDD<String, StructureDataInterface> structures = MmtfImporter.downloadSwissModelsByUniProtIds(uniProtIds, sc);

        structures.foreach(t -> TraverseStructureHierarchy.printStructureData(t._2));
        
        // save as an MMTF-Hadoop Sequence File
 //       MmtfWriter.writeSequenceFile(mmtfPath, sc, structures);

        // close Spark
        sc.close(); 
    }
}
