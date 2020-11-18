# dataflowwc

This dataflow program will take input file path as argument and generate seperate output file for following -

1. Counts of all words
2. Word(s) with the lowest count
3. Word(s) with the highest count
4. Sum of all words

## Instruction to run the program
1. Clone this project as maven project
2. build the project.
3. execute the dataflow with the following command

mvn compile exec:java -Dexec.mainClass=hello.projects.dataflowwc.dataflowwc.WordCountApp -Dexec.args="--runner=DataflowRunner --inputFile=<Input file complete path in gcp bucket>  --outputFileBucketName=<GCP bucket for output file> --gcpTempLocation=<Temp location in GCP bucket, eg- gs://bucketName/temp/> --tempLocation=<Temp location in GCP bucket, eg- gs://bucketName/temp/> --region=us-east1 --project=<ProjectId>"

OR you can create templete with the same command as above without exec and use that template to invoke dataflow from cloud function/ Pub Sub/ etc..

![Alt text](storage.png?raw=true "Seperate files created in storage")

![Alt text](dataflow.png?raw=true "dataflow job will looks like this")
