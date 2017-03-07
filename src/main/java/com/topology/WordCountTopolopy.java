package com.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.bolt.ReportBolt;
import com.bolt.SplitSentenceBolt;
import com.bolt.WordCountBolt;
import com.spout.SentenceSpout;

public class WordCountTopolopy {
	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String TOPOLOPY_NAME = "word-count-topolopy";
	
	public static void main(String[] args) throws Exception{
		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
		WordCountBolt countBolt = new WordCountBolt();
		ReportBolt reportBolt = new ReportBolt();
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(SENTENCE_SPOUT_ID, spout, 2);
		builder.setBolt(SPLIT_BOLT_ID, splitBolt, 2).setNumTasks(4).shuffleGrouping(SENTENCE_SPOUT_ID);
		//builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
		builder.setBolt(COUNT_BOLT_ID, countBolt).shuffleGrouping(SPLIT_BOLT_ID);
		builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);
		
		Config config = new Config();
		config.setNumWorkers(2);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOPY_NAME, config, builder.createTopology());
		
		Thread.sleep(3000);
		
		cluster.killTopology(TOPOLOPY_NAME);
		cluster.shutdown();
	}
}
