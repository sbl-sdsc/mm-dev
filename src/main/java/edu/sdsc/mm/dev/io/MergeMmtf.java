package edu.sdsc.mm.dev.io;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.encoder.AdapterToStructureData;

public class MergeMmtf implements Serializable {
    private static final long serialVersionUID = 4063182623619057069L;

    public static StructureDataInterface MergeStructures(String structureId, StructureDataInterface... structures) {
        for(StructureDataInterface s: structures) {
            if (s.getNumModels() != 1) {
                throw new IllegalArgumentException("ERROR: Cannot merge structures with more than one model");
            }
        }

        AdapterToStructureData complex = new AdapterToStructureData();
        initStructure(structureId, structures, complex);
        addEntityInfo(structures, complex);
        for (StructureDataInterface structure: structures) {
            addStructure(structure, complex);
        }
        complex.finalizeStructure();
        
        return complex;
    }

    private static void initStructure(String structureId, StructureDataInterface[] structures, AdapterToStructureData complex) {
        int nBonds = 0;
        int nAtoms = 0;
        int nGroups = 0;
        int nChains = 0;
        int nModels = 1;
        float resolution = -1;
        float rFree = -1;
        float rWork = -1;
        String title = "";

        for (StructureDataInterface s: structures) {
            nBonds += s.getNumBonds();
            nAtoms += s.getNumAtoms();
            nGroups += s.getNumGroups();
            nChains += s.getNumChains();
            resolution = Math.max(resolution,  s.getResolution());
            rFree = Math.max(rFree,  s.getRfree());
            rWork = Math.max(rFree,  s.getRwork());
            title += s.getTitle();
        }

        String[] experimentalMethods = {"THEORETICAL MODEL"};
        complex.setMmtfProducer("mmtf-spark");
        complex.initStructure(nBonds, nAtoms, nGroups, nChains, nModels, structureId);
        complex.setHeaderInfo(rFree, rWork, resolution, title, "20180101", "20180101", experimentalMethods);
        complex.setModelInfo(0, nChains);    
    }

    private static void addEntityInfo(StructureDataInterface[] structures, AdapterToStructureData complex) {
        int currentOffset = 0;
        int offset = 0;
        for (StructureDataInterface structure: structures) {
            for (int i = 0; i < structure.getNumEntities(); i++) {
                int[] indices = structure.getEntityChainIndexList(i).clone();
 //               System.out.println("offset: " + offset);
                for (int j = 0; j < indices.length; j++) {
                    indices[j] += offset;
                    currentOffset = Math.max(currentOffset, indices[j]);
                }
                complex.setEntityInfo(
                        indices, 
                        structure.getEntitySequence(i), 
                        structure.getEntityDescription(i), 
                        structure.getEntityType(i));
            }
            offset = currentOffset + 1;
        }
    }
    
    private static void addStructure(StructureDataInterface structure, AdapterToStructureData mergedStructure) {
        // Global indices that point into the flat (columnar) data structure
        int chainIndex = 0;
        int groupIndex = 0;
        int atomIndex = 0;

        // create an index that maps a chain to its entity
        int[] chainToEntityIndex = getChainToEntityIndex(structure);

        // Loop over models
        for (int i = 0; i < structure.getNumModels(); i++) {
//            System.out.println("model: " + (i+1));

            // Loop over chains in a model
            for (int j = 0; j < structure.getChainsPerModel()[i]; j++) {

                // Print chain info
                String chainName = structure.getChainNames()[chainIndex]; // this is the chain name used in pdb files
                String chainId = structure.getChainIds()[chainIndex]; // this is also called asym_id in mmCIF           
                int groups = structure.getGroupsPerChain()[chainIndex];
//                System.out.println("chainName: " + chainName + ", chainId: " + chainId + ", groups: " + groups);

                mergedStructure.setChainInfo(chainId, chainName, groups);

                // Print entity info
                String entityType = structure.getEntityType(chainToEntityIndex[chainIndex]);        
                String entityDescription = structure.getEntityDescription(chainToEntityIndex[chainIndex]);
                String entitySequence = structure.getEntitySequence(chainToEntityIndex[chainIndex]); 
//                System.out.println("entity type          : " + entityType);
//                System.out.println("entity description   : " + entityDescription);
//                System.out.println("entity sequence      : " + entitySequence);

                // Loop over groups in a chain
                for (int k = 0; k < structure.getGroupsPerChain()[chainIndex]; k++) {

                    // get group data
                    int groupId = structure.getGroupIds()[groupIndex]; // aka residue number
                    char insertionCode = structure.getInsCodes()[groupIndex];
                    int secStruct = structure.getSecStructList()[groupIndex];
                    int seqIndex = structure.getGroupSequenceIndices()[groupIndex];

                    // Unique groups (residues) are stored only once in a dictionary. 
                    // We need to get the group type to retrieve group information
                    int groupType = structure.getGroupTypeIndices()[groupIndex];    

                    // retrieve group info from dictionary
                    String groupName = structure.getGroupName(groupType);
                    String chemCompType = structure.getGroupChemCompType(groupType);
                    char oneLetterCode = structure.getGroupSingleLetterCode(groupType);

                    int numAtoms = structure.getNumAtomsInGroup(groupType);
                    int numBonds = structure.getGroupBondOrders(groupType).length;

                    mergedStructure.setGroupInfo(groupName, groupId, insertionCode, entityType, numAtoms, numBonds, oneLetterCode, seqIndex, secStruct);

//                    System.out.println("   groupName      : " + groupName);
//                    System.out.println("   oneLetterCode  : " + oneLetterCode);
//                    System.out.println("   seq. index     : " + seqIndex); // index into complete polymer sequence ("SEQRES")
//                    System.out.println("   numAtoms       : " + numAtoms);
//                    System.out.println("   numBonds       : " + numBonds);
//                    System.out.println("   chemCompType   : " + chemCompType);
//                    System.out.println("   groupId        : " + groupId);
//                    System.out.println("   insertionCode  : " + insertionCode);
//                    System.out.println("   DSSP secStruct.: " + DsspSecondaryStructure.getDsspCode(secStruct).getOneLetterCode());
//                    System.out.println();

                    // Loop over atoms in a group retrieved from the dictionary
                    for (int m = 0; m < structure.getNumAtomsInGroup(groupType); m++) {

                        // get atom info
                        int atomId = structure.getAtomIds()[atomIndex];
                        char altLocId = structure.getAltLocIds()[atomIndex];
                        float x = structure.getxCoords()[atomIndex];
                        float y = structure.getyCoords()[atomIndex];
                        float z =structure.getzCoords()[atomIndex]; 
                        float occupancy = structure.getOccupancies()[atomIndex];
                        float bFactor = structure.getbFactors()[atomIndex];

                        // get group specific atom info from the group dictionary
                        String atomName = structure.getGroupAtomNames(groupType)[m];
                        String element = structure.getGroupElementNames(groupType)[m];
                        int charge = structure.getGroupAtomCharges(groupType)[m];

//                        System.out.println("      " + atomId + "\t" + atomName + "\t" + altLocId + "\t" + x + "\t" + y 
//                                + "\t" + z + "\t" + occupancy + "\t" + bFactor + "\t" + element);

                        mergedStructure.setAtomInfo(atomName, atomId, altLocId, x, y, z, occupancy, bFactor, element, charge);

                        atomIndex++; // update global atom index
                    }
                    groupIndex++; // update global group index
                }
                chainIndex++;
            }
        }
    }

    /**
     * Returns an array that maps a chain index to an entity index.
     * @param structureDataInterface structure to be traversed
     * @return index that maps a chain index to an entity index
     */
    private static int[] getChainToEntityIndex(StructureDataInterface structure) {
        int[] entityChainIndex = new int[structure.getNumChains()];

        for (int i = 0; i < structure.getNumEntities(); i++) {
            for (int j: structure.getEntityChainIndexList(i)) {
                entityChainIndex[j] = i;
            }
        }
        return entityChainIndex;
    }
}
