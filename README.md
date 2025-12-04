# FlinkQuery
### Runtime Environment
JDK 11
### Execution Steps
1. Use the dbgen tool in the TPC-H project to generate tbl files for all tables.
2. Use DataGenerator.py to combine the tbl files into a streaming input file named input.csv.
3. Open the project and load dependencies using Maven.
4. Run DataExtractor.java to generate a streaming input file new_input.csv containing insert and delete operations.
5. Run ConquirrelCore.java to generate the streaming output file output.csv.