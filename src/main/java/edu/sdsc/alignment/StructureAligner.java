package edu.sdsc.alignment;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.utils.ColumnarStructureX;
import scala.Tuple2;

public class StructureAligner {

	public static Dataset<Row> getAllVsAllAlignments(JavaPairRDD<String, StructureDataInterface> pdb,
			String alignmentAlgorithm) {
		
		SparkSession session = SparkSession.builder().getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(session.sparkContext());
		
		// extract chainName/ C Alpha coordinates
		List<Tuple2<String, Point3d[]>> chains  = pdb.mapValues(
				s -> new ColumnarStructureX(s,true).getcAlphaCoordinates()).collect();
		
		// create an RDD of all pair indices (0,1), (0,2), ..., (1,2), (1,3), ...
		JavaRDD<Tuple2<Integer, Integer>> pairs = getPairs(sc, chains.size());
		JavaRDD<Row> rows = pairs.flatMap(new StructuralAlignmentMapper(sc.broadcast(chains), alignmentAlgorithm));
		
		// convert rows to a dataset
		return session.createDataFrame(rows, getSchema());
	}
	
	public static Dataset<Row> getOneVsAllAlignments(
			JavaPairRDD<String, StructureDataInterface> targets, 
			JavaPairRDD<String, StructureDataInterface> query,
			String alignmentAlgorithm) {
		
		SparkSession session = SparkSession.builder().getOrCreate();
		@SuppressWarnings("resource") // spark context should not be closed here
		JavaSparkContext sc = new JavaSparkContext(session.sparkContext());
		
		List<Tuple2<String, Point3d[]>> chains = new ArrayList<>();
		
		// extract chainName / C Alpha coordinates of query structure
		chains.addAll(query.mapValues(
				s -> new ColumnarStructureX(s,true).getcAlphaCoordinates()).collect());
       
		// add chainName/ C Alpha coordinate tuples for all other structures
		chains.addAll(targets.mapValues(
				s -> new ColumnarStructureX(s,true).getcAlphaCoordinates()).collect());
		
		// create an RDD indices (0,1), (0,2), ..., (0,n)
		// chain 0 represents the query, and chains 1 ... n are the targets
		List<Tuple2<Integer, Integer>> pairList = new ArrayList<>(chains.size());
		for (int i = 1; i < chains.size(); i++) {
			pairList.add(new Tuple2<Integer, Integer>(0, i));
		}
		
		JavaRDD<Tuple2<Integer, Integer>> pairs = sc.parallelize(pairList, 3*sc.defaultParallelism());
		JavaRDD<Row> rows = pairs.flatMap(new StructuralAlignmentMapper(sc.broadcast(chains), alignmentAlgorithm));
		
		// convert rows to a dataset
		return session.createDataFrame(rows, getSchema());
	}
	
	private static StructType getSchema() {
		boolean nullable = false;
		StructField[] sf = {
				DataTypes.createStructField("Id", DataTypes.StringType, nullable),
				DataTypes.createStructField("length", DataTypes.IntegerType, nullable),
				DataTypes.createStructField("coverage1", DataTypes.IntegerType, nullable),
				DataTypes.createStructField("coverage2", DataTypes.IntegerType, nullable),
				DataTypes.createStructField("rmsd", DataTypes.FloatType, nullable),
				DataTypes.createStructField("tm", DataTypes.FloatType, nullable)
		};

	    return DataTypes.createStructType(sf);
    }
	
	private static JavaRDD<Tuple2<Integer, Integer>> getPairs(JavaSparkContext sc, int n) {
		List<Integer> range = IntStream.range(0, n).boxed().collect(Collectors.toList());
		
		JavaRDD<Integer> pRange = sc.parallelize(range, 6*sc.defaultParallelism());

		return pRange.flatMap(new FlatMapFunction<Integer, Tuple2<Integer,Integer>>() {
			private static final long serialVersionUID = -432662341173300339L;

			@Override
			public Iterator<Tuple2<Integer, Integer>> call(Integer t) throws Exception {
				List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
				
				for (int i = 0; i < t; i++) {
				     pairs.add(new Tuple2<Integer, Integer>(i, t));
				}
				return pairs.iterator();
			}
		}).coalesce(3*sc.defaultParallelism(), true); // TODO does this generate equal sized partitions?
	}
}
