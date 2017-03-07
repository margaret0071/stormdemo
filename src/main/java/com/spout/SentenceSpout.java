package com.spout;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout{
	private ConcurrentHashMap<UUID, Values> pending;
	private SpoutOutputCollector collector;
	private String[] sentences = { 
			"my dog has fleas", 
			"i like cold beverages",
			"the dog ate my homework",
			"don't have a cow man",
			"i don't think i like fleas"
	};
	private int index = 0;
	
	public void nextTuple() {
		Values values = new Values(sentences[index]);
		UUID msgId = UUID.randomUUID();
		this.pending.put(msgId, values);
		this.collector.emit(values, msgId);
		
		index++;
		if(index >= sentences.length){
			index=0;
		}
		Utils.sleep(1);
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		this.collector = arg2;
		this.pending = new ConcurrentHashMap<UUID, Values>();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	@Override
	public void fail(Object msgId) {
		this.collector.emit(this.pending.get(msgId), msgId);
	}

	@Override
	public void ack(Object msgId) {
		this.pending.remove(msgId);
	}
}
