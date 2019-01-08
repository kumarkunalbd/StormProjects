import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt 
{

    private HashMap<String, Long> ReportCounts = null;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) 
    {
        this.ReportCounts = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) 
    {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.ReportCounts.put(word, count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        // this bolt does not emit anything

    }
    public void cleanup() 
    {
        System.out.println("--- FINAL COUNTS ---");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.ReportCounts.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            System.out.println(key + " : " + this.ReportCounts.get(key));
        }
        System.out.println("--------------");
    }
}