package com.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt{
	private OutputCollector collector;

	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for(String word : words){
			this.collector.emit(new Values(word));
		}
		this.collector.ack(tuple);
	}

	public void prepare(Map map, TopologyContext topologycontext,
			OutputCollector outputcollector) {
		this.collector=outputcollector;
	}

	public void declareOutputFields(OutputFieldsDeclarer outputfieldsdeclarer) {
		outputfieldsdeclarer.declare(new Fields("word"));
	}
}
