package com.upgrad.storm;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class SentenceSpoutClass extends BaseRichSpout {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
    private int numberOfAcknowldgedMessages =0;
    private int numberOfFailedMessages =0;
    private long numberofMessages = 0;
    
    
    private String[] sentences = {
        "my dog has fleas",
        "i like cold beverages",
        "the dog ate my homework",
        "dont have a cow man",
        "i dont think i like fleas"
    };
    private int index = 0;
    /* Included ack method to impelenment relaibiity API and get final confirmation of acknowledgement*/
    public void ack(Object msgId) {
    	this.numberOfAcknowldgedMessages = this.numberOfAcknowldgedMessages+1;
    	//System.out.println("___________________ACKED_________________for msgID:"+msgId);
    	//System.out.println("___________________Number Msg Acked:"+String.valueOf(this.numberOfAcknowldgedMessages));
    }
    
    /* Included fail method to impelenment relaibiity API and get final confirmation of acknowledgement*/
    public void fail(Object msgId) {
    	this.numberOfFailedMessages = this.numberOfFailedMessages+1;
    	//System.out.println("___________________Failed_________________for msgID:"+msgId);
    	//System.out.println("___________________Number Msg failed:"+String.valueOf(this.numberOfFailedMessages));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        declarer.declare(new Fields("sentence"));
        //declarer.declareStream(arg0, arg1);
    }

    public void open(Map config, TopologyContext context, 
            SpoutOutputCollector collector) 
    {
        this.collector = collector;
    }

    /* Creating the unique message in Sprout to assign in each emission  */
    public void nextTuple() 
    {
    	this.numberofMessages = this.numberofMessages+1;
    	this.collector.emit(new Values(sentences[index]),sentences[index].subSequence(0,7)+Long.toString(this.numberofMessages));
        //System.out.println("______Message Emiited:"+sentences[0]+Integer.toString(this.numberofMessages));
        index++;
        if (index >= sentences.length) 
        {
            index = 0;
        }
        
        
     }
}