package edu.sdsc.mm.dev.io.demos;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.spark_project.guava.io.Files;

import edu.sdsc.mm.dev.io.MergeMmtf;
import edu.sdsc.mm.dev.io.Molmporter;
import edu.sdsc.mmtf.spark.io.MmtfImporter;
import edu.sdsc.mmtf.spark.io.demos.TraverseStructureHierarchy;
import scala.Tuple2;

public class D3RLigandProteinMerger {

    public static void main(String[] args) throws IOException {
        
        long start = System.nanoTime();
        // instantiate Spark
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("D3RLigandProteinMerger");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
//        String path = "/Users/peter/Downloads/Pose_prediction/417-1-hciq4/";
        String path = "/Users/peter/Downloads/Pose_prediction/";

        JavaPairRDD<String, StructureDataInterface> ligands = Molmporter.importMolFiles(path, sc);
        ligands = ligands.mapToPair(t -> new Tuple2<String, StructureDataInterface>(removeExtension(t._1), t._2));

        JavaPairRDD<String, StructureDataInterface> proteins = MmtfImporter.importPdbFiles(path, sc);  
        proteins = proteins.mapToPair(t -> new Tuple2<String, StructureDataInterface>(removeExtension(t._1), t._2));
        
        JavaPairRDD<String, Tuple2<StructureDataInterface, StructureDataInterface>> pairs = proteins.join(ligands);
        
        JavaPairRDD<String, StructureDataInterface> complexes = pairs.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1, MergeMmtf.MergeStructures(t._1, t._2._1, t._2._2)));
 
        complexes.foreach(t -> TraverseStructureHierarchy.printChainInfo(t._2));
 //       System.out.println("Complexes: " + complexes.count());
 //       complexes.keys().foreach(k -> System.out.println(k));
//        TraverseStructureHierarchy.printChainInfo(complexes.first()._2);
    
        sc.close();
        
        long end = System.nanoTime();
        System.out.println("Time: " + (end-start)/1E9 + " sec.");
    }
    
    private static String removeExtension(String fileName) {
        return fileName.substring(0, fileName.lastIndexOf("."));
    }
}
