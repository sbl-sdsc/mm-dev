package edu.sdsc.mm.dev.io.demos;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mm.dev.io.Molmporter;
import edu.sdsc.mmtf.spark.io.demos.TraverseStructureHierarchy;

public class ImportMolFile {

    public static void main(String[] args) throws IOException {
//        String fileName = "/Users/peter/Downloads/Pose_prediction/417-1-hciq4/3OOF-FXR_36-1.mol";
//        Molmporter mp = new Molmporter();
//        StructureDataInterface structure = mp.readFile(fileName);
//        TraverseStructureHierarchy.demo(structure);
        
        // instantiate Spark
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("ImportPdbFiles");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        List<String> ligandIds = Arrays.asList("BTN");
        JavaPairRDD<String, StructureDataInterface> structures = Molmporter.downloadChemicalComponents(ligandIds, sc);
        structures.foreach(t -> TraverseStructureHierarchy.printStructureData(t._2));
        
        sc.close();
    }

}
