import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt
{
    private OutputCollector collector;
    

    public void prepare(Map config, TopologyContext context, OutputCollector collector)
    {
        this.collector = collector;
    }

    public void execute(Tuple tuple) 
    {
        String sentence = tuple.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for(String word : words)
        {
        	word = word.trim();
        	if(! word.isEmpty()) {
        		this.collector.emit(tuple,new Values(word));
        	}
        }
        
        this.collector.ack(tuple);
        //this.collector.fail(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        declarer.declare(new Fields("word"));
    }
}