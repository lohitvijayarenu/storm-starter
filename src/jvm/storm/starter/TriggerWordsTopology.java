package storm.starter;

import storm.starter.spout.RandomSentenceSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class TriggerWordsTopology {

      public static class SplitSentence implements IRichBolt {
        OutputCollector _collector;
      
        public void prepare(Map conf, TopologyContext context,        
          OutputCollector collector) {
           _collector = collector;
        }
        public Map<String, Object> getComponentConfiguration() {
          return null;
        }
     
        public void execute(Tuple tuple) {
          String sentence = tuple.getString(0);
          for(String word: sentence.split(" ")) {
            _collector.emit(tuple, new Values(word));
          }   
          _collector.ack(tuple);
        } 

        public void cleanup() {
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
          declarer.declare(new Fields("word"));
        }
      }

    
    public static class TriggerWord extends BaseBasicBolt {
        Integer count = 0; 

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            if (word.equals("cow")) {
              count++;
            }
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }
    
    public static void main(String[] args) throws Exception {
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("spout", new RandomSentenceSpout(), 5);
        
        builder.setBolt("split", new SplitSentence(), 8)
                 .shuffleGrouping("spout");
        builder.setBolt("count", new TriggerWord(), 12)
                 .fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(false);

        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {        
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("triggerword-count", conf, builder.createTopology());
        
            Thread.sleep(10000);

            cluster.shutdown();
        }
    }
}
