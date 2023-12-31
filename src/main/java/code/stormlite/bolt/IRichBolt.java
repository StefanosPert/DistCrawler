/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package code.stormlite.bolt;

import java.util.Map;

import code.stormlite.IStreamSource;
import code.stormlite.TopologyContext;
import code.stormlite.routers.IStreamRouter;
import code.stormlite.tuple.Fields;
import code.stormlite.tuple.Tuple;

/** (MS2) An interface for bolts.
 */
public interface IRichBolt extends IStreamSource {
	
	/**
	 * Called when a bolt is about to be shut down
	 */
	public void cleanup();
	
	/**
	 * Processes a tuple
	 * @param input
	 */
	public void execute(Tuple input);
	
	/**
	 * Called when this task is initialized
	 * 
	 * @param stormConf
	 * @param context
	 * @param collector
	 */
	public void prepare(Map<String,String> stormConf,
            TopologyContext context,
            OutputCollector collector);
	
	/**
	 * Called during topology creation: sets the output router
	 * 
	 * @param router
	 */
	public void setRouter(IStreamRouter router);

	/**
	 * Get the list of fields in the stream tuple
	 * 
	 * @return
	 */
	public Fields getSchema();
}
