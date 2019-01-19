package com.upgrad.storm;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCountBoltClass extends BaseRichBolt
{
    private OutputCollector collector;
    private HashMap<String, Long> counts = null;

    public void prepare(Map config, TopologyContext context, 
            OutputCollector collector) 
    {
        this.collector = collector;
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) 
    {
        String word = tuple.getStringByField("word");
        Long count = this.counts.get(word);
        if(count == null)
        {
            count = 0L;
        }
        count++;
        this.counts.put(word, count);
        
        /*Anchoring the SplitSentence Bolt*/

        this.collector.emit(tuple,new Values(word, count));
        
        /*Acknowledging the Tuple*/
        this.collector.ack(tuple);
        
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word", "count"));
    }
}