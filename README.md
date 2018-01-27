## mm-dev
This repository is an incubating area for new methods for the [mmtf-spark](https://github.com/sbl-sdsc/mmtf-spark) project.

## Installation
### Prerequisites
Install mmtf-spark and download Hadoop Sequence files [mmtf-spark](https://github.com/sbl-sdsc/mmtf-spark).

### Install mm-dev
Clone the mm-dev repository and build the project using Maven.

```
cd INSTALL_DIRECTORY
git clone https://github.com/sbl-sdsc/mm-dev.git
cd mm-dev
mvn install
```
The *install* goal will compile, test, and package the project’s code and then copy it into the local dependency repository, which Maven maintains on your local machine.


## Run a Demo using Maven
The Maven **exec** plugin lets you run the main method of a Java class in the project, with the project dependencies automatically included in the classpath.


### Run DrugBankDemo
This demo shows how to create a dataset with drug information from DrugBank.

```
mvn exec:java -Dexec.mainClass="edu.sdsc.mm.dev.datasets.demos.DrugBankDemo"
```

The option `-Dexec.mainClass` specifies the package name and the name of the class with the main method to be executed.


### Run WaterInteractions
This demo shows how to analyze bridging water interactions between a ligand and a protein in the PDB.

```
mvn exec:java -Dexec.mainClass="edu.sdsc.mm.dev.interactions.demos.WaterInteractions" -Dexec.args="-r 2.0 -d 3.0 -b 1.645 -min 4 -max 4 -o pathToOutputDirectory" -DMMTF_FULL="pathToMmtfDirectory/full"
```

The option `-Dexec.mainClass` specifies the package name and the name of main class to be executed.

The option `-Dexec.args` specifies the command line arguments:

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-r 2.0`: minimum resolution of structure

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-d 3.0`: maximum distance for interactions

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-b 1.645`: maximum normalized b-factor (this z-score corresponds to the 90% confidence interval)

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-min 4`: minimum number of interactions

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-max 4`: maximum number of interactions

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-w`: include other waters as interaction partners (not used in this example)

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-o`: path to output directory

The option `-DMMTF_FULL` specifies the location of the PDB archive as a Hadoop Sequence File directory named *full*.



**Note:** You may need to increase the memory allocation pool for a Java Virtual Machine. Use *-Xmx* option to increase the Java heap size to 4G when running the analysis.
```
export MAVEN_OPTS="-Xmx4G"
```
