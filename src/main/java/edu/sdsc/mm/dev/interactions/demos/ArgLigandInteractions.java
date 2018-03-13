package edu.sdsc.mm.dev.interactions.demos;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lower;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.filters.PolymerComposition;
import edu.sdsc.mmtf.spark.filters.Resolution;
import edu.sdsc.mmtf.spark.interactions.ExcludedLigandSets;
import edu.sdsc.mmtf.spark.interactions.GroupInteractionExtractor;
import edu.sdsc.mmtf.spark.interactions.InteractionFilter;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * WaterInteractions calculates interactions at the interface between ligands
 * and protein chains. Command line argument are used to specify criteria
 * for the interactions, such as distance cutoffs.
 * 
 * @author Peter Rose
 *
 */
public class ArgLigandInteractions {

    public static void main(String[] args) throws IOException, ParseException {
        String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmm").format(Calendar.getInstance().getTime());
        long start = System.nanoTime();
        
        // process command line options (defaults are provided)
        CommandLine cmd = getCommandLine(args);
        String outputPath = cmd.getOptionValue("output-path");
        System.out.println(outputPath);
        String resolution = cmd.getOptionValue("resolution", "2");
        String minInteractions = cmd.getOptionValue("min-interactions", "2");
        String maxInteractions = cmd.getOptionValue("max-interactions", "4");
        String distanceCutoff = cmd.getOptionValue("distance-cutoff", "3");
        String bFactorCutoff = cmd.getOptionValue("b-factor-cutoff", "1.645");
        boolean includeWaters = cmd.hasOption("include-waters");

        // get path to MMTF Hadoop Sequence file
        String path = MmtfReader.getMmtfFullPath();
        
        // initialize Spark
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ArgLigandInteractions.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // read PDB structures and filter by resolution and only include proteins
        JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc)
                .filter(new Resolution(0.0, Float.parseFloat(resolution)))
                .filter(new ContainsLProteinChain(true));

        
        // setup interaction criteria
        InteractionFilter filter = new InteractionFilter();
        filter.setDistanceCutoff(Float.parseFloat(distanceCutoff));
        filter.setNormalizedbFactorCutoff(Float.parseFloat(bFactorCutoff));
        filter.setMinInteractions(Integer.parseInt(minInteractions));
        filter.setMaxInteractions(Integer.parseInt(maxInteractions));
        filter.setQueryGroups(true, "ARG");
        filter.setQueryElements(true, "N"); // only use water oxygen
        filter.setTargetElements(true, "O", "N", "S");
        filter.setTargetGroups(false, new HashSet<>(PolymerComposition.AMINO_ACIDS_20));
  
        // exclude "uninteresting" ligands
        Set<String> prohibitedGroups = new HashSet<>();
        prohibitedGroups.addAll(ExcludedLigandSets.ALL_GROUPS);
        if (!includeWaters) {
            prohibitedGroups.add("HOH");
        }
        filter.setProhibitedTargetGroups(prohibitedGroups);
     
        // calculate interactions
        Dataset<Row> data = GroupInteractionExtractor.getInteractions(pdb, filter);

      // only consider interactions with ARG sidechain nitrogens
      data = data.filter("atom0 = 'NE' OR atom0 = 'NH1' OR atom0 = 'NH2'");
      
      // the interacting group should be an organic ligand (LGO)
      data = data.filter("type1 = 'LGO'");
      
      data = data.select("pdbId", 
      "atom0", "groupNum0", "chain0", 
      "atom1", "group1", "groupNum1", "chain1", "distance1");
      
 //     data.show(50);
      
      Dataset<Row> data2 = data;
      Dataset<Row> joint = data.join(data2, 
         (data.col("pdbId").equalTo(data2.col("pdbId")))
         .and(data.col("atom0").notEqual(data2.col("atom0")))
         .and(data.col("groupNum1").equalTo(data2.col("groupNum1"))
         .and(data.col("chain1").equalTo(data2.col("chain1"))
         .and(data.col("atom1").notEqual(data2.col("atom1"))))
         ));
      joint.show(100);
//        data = data.select("pdbId", 
//                "atom0", "groupNum0", "chain0", 
//                "atom1", "groupNum1", "chain1", "distance1",
//                "atom2", "groupNum2", "chain2", "distance2");
//        
//        // only consider interactions with ARG sidechain nitrogens
//        data = data.filter("atom0 = 'NE' OR atom0 = 'NH1' OR atom0 = 'NH2'");
//        
//        // the interacting group should be an organic ligand (LGO)
//        data = data.filter("type1 = 'LGO' AND type2 = 'LGO'").cache();
//        
//        // the two interacting atoms must come from the same group and chain
//        data = data.filter("group1 = group2 AND groupNum1 = groupNum2 AND chain1 = chain2");
//        Dataset<Row> data2 = data;
//        Dataset<Row> joint = data.join(data2, 
//                data.col("pdbId").equalTo(data2.col("pdbId")).and
//               (data.col("groupNum1").equalTo(data2.col("groupNum1")).and
//               (data.col("chain1").equalTo(data2.col("chain1")))
//                ));
//        joint.show(100);
//        RelationalGroupedDataset groupBy = data.groupBy("pdbId", "groupNum0", "chain0", "group1", "groupNum1");
//        groupBy.count().show(1000);
        
        // show some results
//        data.show(50);
//        System.out.println("Hits(all): " + data.count());
//     
//        // save interactions to a .parquet file
//        String waterTag = includeWaters ? "_w" : "";
//        String filename = outputPath + "/arg_lig" + "_r" + resolution 
//                + "_d" + distanceCutoff
//                + "_b" + bFactorCutoff + "_i" + minInteractions + maxInteractions + waterTag + "_" + timeStamp + ".parquet";
//        System.out.println("Saving results to: " + filename);
//        data.coalesce(1).write().mode("overwrite").format("parquet").save(filename);

        
        // exit Spark
        sc.close();

        long end = System.nanoTime();
        System.out.println("Time: " + TimeUnit.NANOSECONDS.toSeconds(end - start) + " sec.");
    }

    /**
     * Remove rows where the water interaction does not include at least one organic ligand (LGO) 
     * and one protein residue (PRO).
     * 
     * TODO need to handle cases of maxInteractions > 4
     * @param data
     * @param maxInteractions
     * @return
     */
    private static Dataset<Row> filterBridgingWaterInteractions(Dataset<Row> data, String maxInteractions) {    
    	if (maxInteractions.compareTo("4") > 0) {
    		throw new IllegalArgumentException("maxInteractions > 4 are not supported, yet");
    	}
    	
        if (maxInteractions.equals("2")) {
            data = data.filter(col("type1").equalTo("LGO").or(col("type2").equalTo("LGO")));
            data = data.filter(col("type1").equalTo("PRO").or(col("type2").equalTo("PRO")));
        } else if (maxInteractions.equals("3")) {
            data = data.filter(col("type1").equalTo("LGO").or(col("type2").equalTo("LGO"))
                    .or(col("type3").equalTo("LGO")));
            data = data.filter(col("type1").equalTo("PRO").or(col("type2").equalTo("PRO"))
                    .or(col("type3").equalTo("PRO")));
        } else if (maxInteractions.equals("4")) {
            data = data.filter(col("type1").equalTo("LGO").or(col("type2").equalTo("LGO"))
                    .or(col("type3").equalTo("LGO")).or(col("type4").equalTo("LGO")));
            data = data.filter(col("type1").equalTo("PRO").or(col("type2").equalTo("PRO"))
                    .or(col("type3").equalTo("PRO")).or(col("type4").equalTo("PRO")));
        }
        return data;
    }
    
    private static CommandLine getCommandLine(String[] args) {
        Options options = new Options();

        options.addOption("h", "help", false, "help");
        options.addOption("o", "output-path", true, "path to output file");
        options.addOption("r", "resolution", true, "minimum resolution of structure");
        options.addOption("d", "distance-cutoff", true, "maximum distance for interactions");
        options.addOption("b", "b-factor-cutoff", true, "maximum normalized b-factor");
        options.addOption("min", "min-interactions", true, "minimum number of interactions");
        options.addOption("max", "max-interactions", true, "maximum number of interactions");
        options.addOption("w", "include-waters", false, "include water-water interactions");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println("ERROR: invalid command line arguments: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(ArgLigandInteractions.class.getSimpleName(), options);
            System.exit(-1);
        }

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(ArgLigandInteractions.class.getSimpleName(), options);
            System.exit(1);
        }
        
        // TODO this check does not work??
//        if (!cmd.hasOption("output-path")) {
//            System.err.println("ERROR: no output path specified!");
//            HelpFormatter formatter = new HelpFormatter();
//            formatter.printHelp(WaterInteractions.class.getSimpleName(), options);
//            System.exit(1);
//        }

        return cmd;
    }
}
