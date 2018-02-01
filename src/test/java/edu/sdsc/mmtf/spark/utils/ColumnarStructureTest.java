package edu.sdsc.mmtf.spark.utils;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mm.dev.utils.ColumnarStructure;
import edu.sdsc.mmtf.spark.io.MmtfReader;

public class ColumnarStructureTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;

	@Before
	public void setUp() throws Exception {
      	SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ColumnarStructureTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);

	    List<String> pdbIds = Arrays.asList("1STP");
		pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void testGetxCoords() {
		StructureDataInterface s = pdb.filter(t -> t._1().equals("1STP")).values().collect().get(0);
        ColumnarStructure aa = new ColumnarStructure(s, true);
		assertEquals(26.260, aa.getxCoords()[20], 0.001);
	}
	
	@Test
	public void testGetElements() {
		StructureDataInterface s = pdb.filter(t -> t._1().equals("1STP")).values().collect().get(0);
        ColumnarStructure aa = new ColumnarStructure(s, true);
		assertEquals("C", aa.getElements()[20]);
	}
	
	@Test
	public void testGetAtomNames() {
		StructureDataInterface s = pdb.filter(t -> t._1().equals("1STP")).values().collect().get(0);
        ColumnarStructure aa = new ColumnarStructure(s, true);
		assertEquals("CG2", aa.getAtomNames()[900]);
	}
	
	@Test
	public void testGetGroupNames() {
		StructureDataInterface s = pdb.filter(t -> t._1().equals("1STP")).values().collect().get(0);
        ColumnarStructure aa = new ColumnarStructure(s, true);
		assertEquals("VAL", aa.getGroupNames()[900]);
	}
	
	@Test
	public void testIsPolymer() {
		StructureDataInterface s = pdb.filter(t -> t._1().equals("1STP")).values().collect().get(0);
        ColumnarStructure aa = new ColumnarStructure(s, true);
		assertEquals(true, aa.isPolymer()[100]); // chain A
		assertEquals(false, aa.isPolymer()[901]); // BTN
		assertEquals(false, aa.isPolymer()[917]); // HOH
	}

	@Test
	public void testGetGroupNumbers() {
			StructureDataInterface s = pdb.filter(t -> t._1().equals("1STP")).values().collect().get(0);
	        ColumnarStructure aa = new ColumnarStructure(s, true);
			assertEquals("130", aa.getGroupNumbers()[877]);
	}
	
	@Test
	public void testGetChainIds() {
			StructureDataInterface s = pdb.filter(t -> t._1().equals("1STP")).values().collect().get(0);
	        ColumnarStructure aa = new ColumnarStructure(s, true);
			assertEquals("A", aa.getChainIds()[100]);
			assertEquals("B", aa.getChainIds()[901]); // BTN
			assertEquals("C", aa.getChainIds()[917]); // HOH
	}
	
	@Test
	public void testGetChemCompTypes() {
			StructureDataInterface s = pdb.filter(t -> t._1().equals("1STP")).values().collect().get(0);
	        ColumnarStructure aa = new ColumnarStructure(s, true);
			assertEquals("PEPTIDE LINKING", aa.getChemCompTypes()[100]);
			assertEquals("NON-POLYMER", aa.getChemCompTypes()[901]); // BTN
			assertEquals("NON-POLYMER", aa.getChemCompTypes()[917]); // HOH
	}
	
	@Test
	public void testGetEntityTypes() {
			StructureDataInterface s = pdb.filter(t -> t._1().equals("1STP")).values().collect().get(0);
	        ColumnarStructure aa = new ColumnarStructure(s, true);
			assertEquals("PRO", aa.getEntityTypes()[100]);
			assertEquals("LGO", aa.getEntityTypes()[901]); // BTN
			assertEquals("WAT", aa.getEntityTypes()[917]); // HOH
	}
}
