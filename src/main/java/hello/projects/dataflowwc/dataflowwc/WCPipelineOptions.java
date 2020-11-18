package hello.projects.dataflowwc.dataflowwc;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface WCPipelineOptions extends DataflowPipelineOptions {

	@Description("GCP path of file is required")
	@Validation.Required
	@Default.String("gs://wcbckt/inputFile.txt")
	ValueProvider<String> getInputFile();
	
	void setInputFile(ValueProvider<String> value);

	
	@Description("Setting of GCP Bucket name for output file and is required")
	@Validation.Required
	@Default.String("gs://wcbckt")
	ValueProvider<String> getOutputFileBucketName();
	
	void setOutputFileBucketName(ValueProvider<String> value);
}
