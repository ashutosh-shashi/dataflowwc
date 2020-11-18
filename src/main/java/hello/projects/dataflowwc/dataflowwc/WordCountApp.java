package hello.projects.dataflowwc.dataflowwc;

import java.io.Serializable;
import java.util.Comparator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class WordCountApp 
{
	static final Logger log = LoggerFactory.getLogger(WordCountApp.class);
	
	public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";
		
    public static void main( String[] args )
    {
    	log.info("Setting pipeline configurations");
    	PipelineOptionsFactory.register(WCPipelineOptions.class);
    	WCPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WCPipelineOptions.class);
    	log.info("Pipeline Created with provided options");
    	
    	runWordCnt(options);
    }
    
    static void runWordCnt(WCPipelineOptions options) {
        Pipeline p = Pipeline.create(options);
        PCollection<KV<String, Long>> wordCollection = p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
            .apply(new CntWords());

        log.info("Counts of all words");
        wordCollection.apply(Keys.<String>create())
        .apply(Count.globally())
        .apply(MapElements.via(new FormatAsCountOfAllWordsFn()))
        .apply("CountOfAllWords", TextIO.write().to(options.getOutputFileBucketName()).withSuffix("CountOfAllWords.txt").withoutSharding());
        
        log.info("Word(s) with the lowest count");
        wordCollection.apply(Combine.globally(Min.of(new KVComparator())))
        .apply(MapElements.via(new FormatAsTextFn()))
        .apply("LowestCount", TextIO.write().to(options.getOutputFileBucketName()).withSuffix("LowestCount.txt").withoutSharding());
        
        log.info("Word(s) with the highest count");
        wordCollection.apply(Combine.globally(Max.of(new KVComparator())))
        .apply(MapElements.via(new FormatAsTextFn()))
        .apply("HighestCount", TextIO.write().to(options.getOutputFileBucketName()).withSuffix("HighestCount.txt").withoutSharding());

        log.info("Sum of all words");
        wordCollection.apply(Values.<Long>create())
        .apply(Sum.longsGlobally())
        .apply(MapElements.via(new FormatAsSumTextFn()))
        .apply("SumOfAllWords", TextIO.write().to(options.getOutputFileBucketName()).withSuffix("SumOfAllWords.txt").withoutSharding());
        
        p.run().waitUntilFinish();
      }

    public static class CntWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {	  
      @Override
	  public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
		    PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));
	    PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());
	
	    return wordCounts;
	  }
    }
    
    static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
            Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
          lineLenDist.update(element.length());
          if (element.trim().isEmpty()) {
            emptyLines.inc();
          }

          // Split the line into words.
          String[] words = element.split(TOKENIZER_PATTERN, -1);

          // Output each word encountered into the output PCollection.
          for (String word : words) {
            if (!word.isEmpty()) {
              receiver.output(word);
            }
          }
        }
      }
    
    public static class KVComparator implements Comparator<KV<String,Long>>, Serializable {
        @Override
        public int compare(KV<String, Long> o1, KV<String, Long> o2) {
         return   o1.getValue().compareTo(o2.getValue());
        }
    }
    
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
          return input.getKey() + ": " + input.getValue();
        }
      }
    
    public static class FormatAsSumTextFn extends SimpleFunction<Long, String> {
        @Override
        public String apply(Long input) {
          return "Sum of all words count is" + ": " + input;
        }
      }
    
    public static class FormatAsCountOfAllWordsFn extends SimpleFunction<Long, String> {
        @Override
        public String apply(Long input) {
          return "Count of all words count is" + ": " + input;
        }
      }
}
