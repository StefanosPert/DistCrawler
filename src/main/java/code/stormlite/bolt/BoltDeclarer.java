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

import java.io.Serializable;

import code.stormlite.routers.FieldBased;
import code.stormlite.routers.IStreamRouter;
import code.stormlite.routers.RoundRobin;
import code.stormlite.tuple.Fields;

/**
 * (MS2) This determines how we route messages to the next
 * operator, e.g., round-robin, hash-based, ... 
 * 
 * @author zives
 *
 */
public class BoltDeclarer implements Serializable {
	
	public static final String SHUFFLE = "shuffle";
	public static final String FIELDS = "fields";
	
	/**
	 * The stream ID
	 */
	String stream;
	
	/**
	 * The kind of stream (how it routes among multiple executors)
	 */
	String type;
	
	/**
	 * Fields used for sharding
	 */
	Fields shardFields;
	
	/**
	 * Where the stream messages are handled
	 */
	IStreamRouter router;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public BoltDeclarer() {
	}

	public BoltDeclarer(String typ) {
		setType(typ);
	}
	
	/**
	 * Get the type of the stream
	 * 
	 * @return
	 */
	public String getType() { 
		return type; 
	}
	
	/**
	 * Set the type of stream
	 * @param typ
	 */
	public void setType(String typ) {
		this.type = typ;
	} 
	
	/**
	 * The name of the stream
	 * @return
	 */
	public String getStream() {
		return stream;
	}
	
	/**
	 * If we have field-based grouping, we need to know
	 * the fields used to shard the data
	 * 
	 * @return
	 */
	public Fields getShardingFields() {
		return shardFields;
	}
	
	/**
	 * Round robin
	 * 
	 * @param key The stream name
	 */
	public void shuffleGrouping(String key) {
		this.stream = key;
		setType(SHUFFLE);
	}

	/**
	 * Partition (shard) by fields
	 * 
	 * @param key The stream name
	 * @param fields The fields to group and shard by
	 */
	public void fieldsGrouping(String key, Fields fields) {
		this.stream = key;
		shardFields = fields;
		setType(FIELDS);
	}

	/**
	 * Creates (or returns) a "router" that funnels our
	 * output stream to the next destination (which might
	 * have multiple executors).
	 *  
	 * @return
	 */
	public IStreamRouter getRouter() {
		if (router == null)
				// Round-robin is straightforward
			if (getType().equals(SHUFFLE)) {
				router = new RoundRobin();
				
				// If we are sharding by fields, look up
				// the indices within the schema of the stream
			} else if (getType().equals(FIELDS)) {
				router = new FieldBased(shardFields);
			}
		
		return router;
	}
}
