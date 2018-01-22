## mm-dev
This repository is a working area for incubating new methods for the mmtf-spark project.

## Install from git repository
You can get the latest source code using git. Then you can execute the *install* goal with [Maven](http://maven.apache.org/guides/getting-started/index.html#What_is_Maven) to build the project.
```

$ git clone https://github.com/sbl-sdsc/mmtf-spark.git
$ cd mm-dev
$ mvn install

$ git clone https://github.com/sbl-sdsc/mm-dev.git
$ cd mm-dev
$ mvn install
```
The *install* goal will compile, test, and package the project’s code and then copy it into the local dependency repository which Maven maintains on your local machine.

If you use Maven for the first time, these links can be useful:</br>
[Where to download Maven](http://maven.apache.org/download.cgi)</br>
[How to install Maven](http://maven.apache.org/install.html)

## How to run a demo using Maven
Maven **exec** plugin lets you run the main method of a Java class in the project, with the project dependencies automatically included in the classpath.

### Run DrugBankDemo
```
This demo shows how to create a dataset with drug information from DrugBank.

mvn exec:java -Dexec.mainClass="edu.sdsc.mm.dev.datasets.demos.DrugBankDemo" -Dexec.args="arg1 arg2"
```

### Run WaterInteractions
```
This demo shows how to analyze bridging waters between a ligand and a protein in the PDB.

mvn exec:java -Dexec.mainClass="edu.sdsc.mm.dev.datasets.demos.WaterInteractions" -Dexec.args="-r 2.0 -d 3.0 -b 1.645 -min 4 -max 4 -o pathToOuputDirectory" -DMMTF_FULL="pathToMMTFFullDirectory"
```

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-r 2.0`: minimum resolution of structure

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-d 3.0`: maximum distance for interactions

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-b 1.645`: maximum normalized b-factor (this z-score corresponds to the 90% confidence interval)

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-min 4`: minimum number of interactions

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-max 4`: maximum number of interactions

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`-w`: include other water in interactions

**Note:** You may need to increase the memory allocation pool for a Java Virtual Machine. Use *-Xmx* option to increase the Java heap size to 4G when running the analysis.
```
export MAVEN_OPTS="-Xmx4G"
```

## How to get 3D structures in MMTF format
Download a Hadoop sequence file with the PDB structures in MMTF format from mmtf.rcsb.org. You can download this file using a web browse, or one of the following commands:
```
$ wget http://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
$ tar -xvf full.tar
```
or

$ curl -O http://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
$ tar -xvf full.tar
```
This will get and unpack the content of the Hadoop Sequence File to a full folder. Set the MMTF_FULL property to the path of this folders (see example above)