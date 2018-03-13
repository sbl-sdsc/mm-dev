package edu.sdsc.mm.dev.io;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.DsspType;
import org.rcsb.mmtf.encoder.AdapterToStructureData;

import scala.Tuple2;

public class Molmporter implements Serializable {
    private static final long serialVersionUID = -1799083494996785389L;
    private BufferedReader bin = null;
    // temporary variables used when reading a molecule
    //        private SmallMolecule currentMolecule;
    private static final String CCD_URL = "http://files.rcsb.org/ligands/download/";
    private AdapterToStructureData structure;
    private String structureId;
    private String line;
    private String moleculeName;
    private int atomCount;
    private int bondCount;
    private String version;
    private static final String DELIMITER = "$$$$";
    private static final String[] validFileExtensions = {".sdf", ".sd", ".mol"};

    public static boolean canRead(String fileName) {
        String name = fileName;
        if (name.endsWith(".gz")) {
            name = name.substring(0, name.length() - 3);
        }
        for (String extension : validFileExtensions) {
            if (name.toLowerCase().endsWith(extension)) {
                return true;
            }
        }
        return false;
    }

    /**
     * <p>
     * Depending on the input data, the generated data model may be partially
     * incomplete.
     * 
     * @param path
     *            Path to uncompressed or compressed PDB files
     * @param sc
     *            Spark context
     * @return structure data with the file path as the key and the structure as
     *         the value
     */
    public static JavaPairRDD<String, StructureDataInterface> importMolFiles(String path, JavaSparkContext sc) {
        List<File> files = getFiles(path);
        return sc.parallelize(files)
                .mapToPair(
                        t -> new Tuple2<String, StructureDataInterface>(
                                t.toString(), new Molmporter().readFile(t.toString())))
                .filter(t -> t._2 != null);
    }

    /**
     * Downloads SWISS-MODEL homology models for a list of UniProtIds. 
     * Non-matching UniProtIds are ignored.
     * 
     * <p>Example
     * <pre>
     * {@code
     * List<String> uniProtIds = Arrays.asList("P36575","P24539","O00244","P18846","Q9UII2");
     * JavaPairRDD<String, StructureDataInterface> structures = MmtfImporter.downloadSwissModelsByUniProtIds(uniProtIds, sc);
     * }
     * </pre></code>
     * 
     * @param uniProtIds
     *            List of UniProtIds (upper case)
     * @param sc
     *            Spark context
     * @return structure data  key = UniProtId, value = structure
     * @see <a href="https://swissmodel.expasy.org/docs/repository_help#smr_api"">SWISS-MODEL API</a>.
     */
    public static JavaPairRDD<String, StructureDataInterface> downloadChemicalComponents(List<String> ligandIds,
            JavaSparkContext sc) {
        return sc.parallelize(ligandIds)
                .mapToPair(t -> new Tuple2<String, StructureDataInterface>(
                        t,
                        new Molmporter().readUrl(CCD_URL + t + "_ideal.sdf")))
                .filter(t -> t._2 != null);
    }

    public String[] getValidFileExtensions() {
        return validFileExtensions;
    }

    public AdapterToStructureData readUrl(String url) throws IOException {
        URL u = new URL(url);
        InputStream is = null;
        try {
            is = u.openStream();
        } catch (IOException e) {
            return null;
        }
        AdapterToStructureData structure = read(is, url);
        return structure;
    }

    public AdapterToStructureData readFile(String fileName) throws IOException {
        InputStream is = new FileInputStream(fileName);
        AdapterToStructureData structure = read(is, fileName);
        is.close();
        return structure;
    }


    public AdapterToStructureData read(String molString, String structureId) throws IOException {
        InputStream is = new ByteArrayInputStream(molString.getBytes());
        read(is, structureId);
        is.close();
        return structure;
    }

    public AdapterToStructureData read(InputStream inputStream, String structureId) throws IOException {
        this.structureId = structureId;
        bin = new BufferedReader(new InputStreamReader(inputStream));

        atomCount = 0;
        bondCount = 0;
        moleculeName = "";

        readHeader();
        readCtab();

        bin.close();
        return structure;
    }

    private void readHeader() throws IOException {
        //          CTFile Format 2005
        //          The Header Block

        //          Line 1:Molecule name.
        //                    This line is unformatted, but like all other lines in a molfile may not extend
        //                    beyond column 80. If no name is available, a blank line must be present.
        //                    Caution: This line must not contain any of the reserved tags that identify any
        //                    of the other CTAB file types such as $MDL (RGfile), $$$$ (SDfile record separator),
        //                    $RXN (rxnfile), or $RDFILE (RDfile headers).
        //            Line 2:
        //               This line has the format:
        //                IIPPPPPPPPMMDDYYHHmmddSSssssssssssEEEEEEEEEEEERRRRRR
        //            (   FORTRAN: A2<--A8--><---A10-->A2I2<--F10.5-><---F12.5--><-I6-> )
        //               User'line first and last initials (l),
        //               program name (P),
        //               date/time (M/D/Y,H:m),
        //               dimensional codes (d),
        //               scaling factors (S, line),
        //               energy (E) if modeling program input,
        //               internal registry number (R) if input through MDL form.
        //
        //               A bank line can be substituted for line 2. If the internal registry number is more than
        //               6 digits long, it is stored in an M REG line (described in Chapter 3).
        //
        //               Line 3:
        //                     A line for comments.
        //                     If no comment is entered, a blank line must be present.
        if ((line = bin.readLine()) != null) {
            moleculeName = line.trim();
        } else {
            throw new IOException("Empty header block: line 1");
        }
        if ((line = bin.readLine()) != null) {
            // ignore this line for now
            ;
        } else {
            throw new IOException("Empty header block: line 2");
        }
        if ((line = bin.readLine()) != null) {
            // ignore this line for now
            ;
        } else {
            throw new IOException("Empty header block: line 3");
        }
    }

    private void readCounts() throws IOException {
        //            CTFile Format 2005
        //            The Counts Line
        //            aaabbblllfffcccsssxxxrrrpppiiimmmvvvvvv
        //                      1         2         3         4
        //            01234567890123456789012345678901234567890
        //            Where::
        //                aaa= number of atoms (current max 255)*[Generic]
        //                bbb= number of bonds (current max 255)*[Generic]
        //                lll= number of atom lists (max 30)* [Query]
        //                fff= (obsolete)
        //                ccc= chiral flag: 0=not chiral, 1=chiral[Generic]
        //                sss= number of stext entries[MDL ISIS/Desktop]
        //                xxx= (obsolete)
        //                rrr= (obsolete)
        //                ppp= (obsolete)
        //                iii= (obsolete)
        //                mmm= number of lines of additional properties, including the M END line.
        //                     No longer supported, the default is set to 999.[Generic]
        //
        //                * These limits apply to MACCS-II, REACCS, and the MDL ISIS/Host Reaction Gateway,
        //                but not to the MDL ISIS/Host Molecule Gateway or MDL ISIS/Desktop.
        //
        //                For example, the counts line in the Ctab shown in the previous figure shows six atoms,
        //                five bonds, the CHIRAL flag on, and three lines in the properties block:
        //                6 5 0 0 1 0 3 V2000

        if ((line = bin.readLine()) != null) {
            if (line.length() < 32) {
                if ((line = bin.readLine()) == null) {
                    return;
                    //                need to figure out why this line is sometimes printed
                    //                        System.out.println("incomplete counts line");
                }
            }
            int chiralValue = 0;
            try {
                atomCount = Integer.parseInt(line.substring(0, 3).trim());
                bondCount = Integer.parseInt(line.substring(3, 6).trim());
                //                   chiralValue = Integer.parseInt(line.substring(12, 15).trim());
            } catch (NumberFormatException e) {
                throw new IOException("Invalid counts line: " + line);
            }

            //                chiralFlag = (chiralValue == 0) ? false : true;
            // some file are missing the version info
            if (line.length() >= 38) {
                version = line.substring(33, 39);
            } else {
                version = "";
            }
            //                    System.out.println("#a: " + atomCount + " #b: " + bondCount + " Chiral" + chiralFlag + " Version: " + version);
        }
    }

    private void readCtab() throws IOException {
        //            if (line == null) return; // this statement terminates reading her
        //            if line 3 of the sdf file is empty, which sometimes occurs.
        readCounts();
        if (atomCount == 0) {
            throw new IOException("Molecule with zero atoms.");//      initiate molecule with the exact capacity needed to hold atoms
        }

        structure = new AdapterToStructureData();
        structure.initStructure(bondCount, atomCount, 1, 1, 1, structureId);
        structure.setHeaderInfo(99, 99, 99, moleculeName, "20180101", "20180101", new String[]{"THEORETICAL MODEL"});
        structure.setModelInfo(0, 1);
        structure.setChainInfo("L", "L", 1);
        structure.setEntityInfo(new int[]{0}, "-1", moleculeName, "non-polymer");
        structure.setGroupInfo("LIG", 1, ' ', "non-polymer", atomCount, bondCount, 'X', -1, -1);
        readAtoms();
        readBonds();
        structure.finalizeStructure();
    }

    private void readAtoms() throws IOException {
        //            CTFile Format 2005
        //          The Atom Block
        //          The Atom Block is made up of atom lines, one line per atom with the following format:
        //          xxxxx.xxxxyyyyy.yyyyzzzzz.zzzz aaaddcccssshhhbbbvvvHHHrrriiimmmnnneee
        //                    1         2         3         4         5         6         7
        //          01234567890123456789012345678901234567890123456789012345678901234567890
        //
        //          where the values are described in the following table:
        //          Field    Meaning                  Values                                   Notes
        //          ---------------------------------------------------------------------------------------------------------------------------
        //          x y z    atom coordinates                                                  [Generic]
        //          aaa      atom symbol              entry in periodic table                  [Generic, Query, 3D, Rgroup]
        //                                            or L for atom list, A, Q,
        //                                            * for unspecified atom,
        //                                            and LP for lone pair,
        //                                            or R# for Rgroup label
        //          dd       mass difference          -3, -2, -1, 0, 1, 2, 3, 4
        //                                            (0 if value beyond these limits)         [Generic] Difference from mass in
        //                                                                                     periodic table. Wider range of values
        //                                                                                     allowed by M ISO line, below. Retained
        //                                                                                     for compatibility with older Ctabs,
        //                                                                                     M ISO takes precedence
        //          ccc      charge                   0 = uncharged or value other than these, [Generic] Wider range of values in
        //                                            1 = +3, 2 = +2, 3 = +1,                  M CHG and M RAD lines below.
        //                                            4 = doublet radical,                     Retained for compatibility with
        //                                            5 = -1, 6 = -2, 7 = -3                   older Ctabs, M CHG and M RAD lines take precedence.
        //          sss      atom stereo parity       0 = not stereo, 1 = odd, 2 = even,       [Generic] Ignored when read.
        //                                            3 = either or unmarked stereo center
        //          hhh      hydrogen count + 1       1 = H0, 2 = H1, 3 = H2, 4 = H3,          H0 means no H atoms allowed unless
        //                                                                                     explicitly drawn. Hn means atom must
        //                                                                                     have n or more H line in excess of explicit Hline.
        //          bbb      stereo care box          0 = ignore stereo configuration of       Double bond stereochemistry is considered
        //                                            this double bond atom,                   during SSS only if both ends of the bond
        //                                            1 = stereo configuration of double bond  are marked with stereo care boxes.
        //                                            atom must match[Query]
        //          vvv      valence                  0 = no marking (default)                 [Generic] Shows number of
        //                                            (1 to 14) = (1 to 14) 15 = zero valence  bonds to this atom, including
        //                                                                                     bonds to implied H line.
        //          HHH      H0 designator            0 = not specified, 1 = no H atoms        [MDL ISIS/Desktop]
        //                                                                                     Redundant with hydrogen count
        //                                                                                     information. May be
        //                                                                                     unsupported in future releases
        //                                                                                     of Elsevier MDL software.
        //          rrr      Not used
        //          iii      Not used
        //          mmm      atom-atom mapping        1 - number of atoms                      [Reaction]
        //          nnn      inversion/retention flag 0 = property not applied                 [Reaction]
        //                                            1 = configuration is inverted,
        //                                            2 = configuration is retained,
        //          eee      exact change flag        0 = property not applied,                [Reaction, Query]
        //                                            1 = change on atom must be exactly as
        //                                            shown
        for (int i = 0; i < atomCount; i++) {
            line = bin.readLine();


            float x = 0.0f;
            float y = 0.0f;
            float z = 0.0f;
            try {
                x = Float.parseFloat(line.substring(0, 10));
                y = Float.parseFloat(line.substring(10, 20));
                z = Float.parseFloat(line.substring(20, 30));
            } catch (IllegalArgumentException e) {
                throw new IOException(e.getMessage());
            }

            String elementSymbol = line.substring(31, 34).trim();
            structure.setAtomInfo(elementSymbol+(i+1), i+1, ' ', x, y, z, 1, 0, elementSymbol, 0);
        }
    }

    private void readBonds() throws IOException {
        //            CTFile Format 2005
        //            The Bond Block
        //            The Bond Block is made up of bond lines, one line per bond, with the following format:
        //            111222tttsssxxxrrrccc
        //                      1         2
        //            012345678901234567890
        //
        //            where the values are described in the following table:
        //          Field    Meaning                  Values                                   Notes
        //          ---------------------------------------------------------------------------------------
        //          111      first atom number        1 - number of atoms                      [Generic]
        //          222      second atom number       1 - number of atoms                      [Generic]
        //          sss      bond stereo              Single bonds: 0 = not stereo,            [Generic]
        //                                            1 = Up, 4 = Either,
        //                                            6 = Down,
        //                                            Double bonds: 0 = Use x-, y-, z-coords
        //                                            from atom block to determine cis or trans,
        //                                            3 = Cis or trans (either) double bond
        //          xxx      not used
        //          rrr      bond topology            0 = Either, 1 = Ring, 2 = Chain          [Query]  SSS queries only.
        //          ccc      reacting center status   0 = unmarked, 1 = a center,              [Reaction, Query]
        //                                            -1 = not a center,
        //                                            Additional: 2 = no change,
        //                                            4 = bond made/broken,
        //                                            8 = bond order changes
        //                                            12 = 4+8 (both made/broken and changes);
        //                                            5 = (4 + 1), 9 = (8 + 1),
        //                                            and 13 = (12 + 1)
        //                                            are also possible
        //
        for (int i = 0; i < bondCount; i++) {
            line = bin.readLine();
            int index1 = Integer.parseInt((line.substring(0, 3)).trim()) - 1;
            int index2 = Integer.parseInt((line.substring(3, 6)).trim()) - 1;

            int bondOrder = Integer.parseInt((line.substring(6, 9)).trim());
            structure.setGroupBond(index1-1, index2-1, bondOrder);
        }

    }

    /**
     * Get list of files from the path
     * 
     * @param path
     *            File path
     * @return list of files in the path
     */
    private static List<File> getFiles(String path) {
        List<File> fileList = new ArrayList<File>();
        for (File f : new File(path).listFiles()) {
            if (f.isDirectory()) {
                fileList.addAll(getFiles(f.toString()));
            } else {
                if (canRead(f.getName()))
                    fileList.add(f);
            }
        }
        return fileList;
    }
}
