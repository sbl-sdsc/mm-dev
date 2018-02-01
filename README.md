## mm-dev
This repository is an incubating area for new methods for the [mmtf-spark](https://github.com/sbl-sdsc/mmtf-spark) project.

## Installation
### Prerequisites
Install mmtf-spark, its prerequisites, and download MMTF-Hadoop Sequence files [mmtf-spark installation guide](https://github.com/sbl-sdsc/mmtf-spark#installation).

### Install mm-dev
Clone the mm-dev repository and build the project using Maven.

```
cd INSTALL_PATH
git clone https://github.com/sbl-sdsc/mm-dev.git
cd mm-dev
mvn install
```
The *install* goal will compile, test, and package the project’s code and then copy it into the local dependency repository, which Maven maintains on your local machine.


## Run a Demo Application using spark-submit
This demo shows how calculate bridging water interactions between a ligand and a protein in the PDB.

```
spark-submit --master local --class edu.sdsc.mm.dev.interactions.demos.WaterInteractions  INSTALL_PATH/mm-dev/target/mm-dev-0.0.1-SNAPSHOT.jar -r 2.0 -d 3.0 -b 1.645 -min 4 -max 4 -o water
```

The option --class specifies the class path and name of the main class (WaterInteractions)

The options after the .jar file are command line arguments for WaterInteractions:

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-r 2.0`: maximum resolution of structure

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-d 3.0`: maximum distance for interactions

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-b 1.645`: maximum normalized b-factor (this z-score corresponds to the 90% confidence interval)

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-min 4`: minimum number of interactions

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-max 4`: maximum number of interactions

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-w`: include other waters as interaction partners (not used in this example)

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-o`: path to output directory

