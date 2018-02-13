package edu.sdsc.mm.dev.io.demos;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.SwissModelDataset;
import edu.sdsc.mmtf.spark.io.MmtfImporter;

/**
 * This demo shows how to access metadata for SWISS-MODEL homology
 * models. 
 * 
 * <p>
 * References:
 * <p>
 * Bienert S, Waterhouse A, de Beer TA, Tauriello G, Studer G, Bordoli L,
 * Schwede T (2017). The SWISS-MODEL Repository - new features and
 * functionality, Nucleic Acids Res. 45(D1):D313-D319.
 * <a href="https://dx.doi.org/10.1093/nar/gkw1132">doi:10.1093/nar/gkw1132</a>.
 * 
 * <p>
 * Biasini M, Bienert S, Waterhouse A, Arnold K, Studer G, Schmidt T, Kiefer F,
 * Gallo Cassarino T, Bertoni M, Bordoli L, Schwede T(2014). The SWISS-MODEL
 * Repository - modelling protein tertiary and quaternary structure using
 * evolutionary information, Nucleic Acids Res. 42(W1):W252–W258.
 * <a href="https://doi.org/10.1093/nar/gku340">doi:10.1093/nar/gku340</a>.
 * 
 * @author Peter Rose
 * @since 0.2.0
 * 
 */
public class SwissModelDatasetToStructure {

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder().master("local[*]").appName(SwissModelDatasetToStructure.class.getSimpleName())
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
       
        List<String> uniProtIds = Arrays.asList("P36575","P24539","O00244","P18846","Q9UII2");
        Dataset<Row> ds = SwissModelDataset.getSwissModels(uniProtIds);
        ds.show();

        ds = ds.filter("qmean > -2.5 AND coverage > 0.5");
        List<String> urls = ds.select("coordinates").as(Encoders.STRING()).collectAsList();
        
        System.out.println(urls);
        JavaPairRDD<String, StructureDataInterface> models = MmtfImporter.downloadSwissModelsByUrls(urls, sc);
       
        models.foreach(t -> System.out.println(t._2.getEntitySequence(0)));
      
        spark.close(); 
    }
}