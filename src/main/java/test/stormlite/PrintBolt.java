package test.stormlite;

import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import code.stormlite.OutputFieldsDeclarer;
import code.stormlite.TopologyContext;
import code.stormlite.bolt.IRichBolt;
import code.stormlite.bolt.OutputCollector;
import code.stormlite.routers.IStreamRouter;
import code.stormlite.tuple.Fields;
import code.stormlite.tuple.Tuple;

/**
 * A trivial bolt that simply outputs its input stream to the
 * console
 * 
 * @author zives
 *
 */
public class PrintBolt implements IRichBolt {
	static Logger log = Logger.getLogger(PrintBolt.class);
	
	Fields myFields = new Fields();

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the PrintBolt, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

	@Override
	public void cleanup() {
		// Do nothing

	}

	@Override
	public void execute(Tuple input) {
		System.out.println(getExecutorId() + ": " + input.toString());
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// Do nothing
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(IStreamRouter router) {
		// Do nothing
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}

}
