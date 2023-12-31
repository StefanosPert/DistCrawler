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
package code.stormlite.tasks;

import java.util.Queue;

import code.stormlite.spout.IRichSpout;

/**
 * (MS2) This is a simple task that retrieves a tuple from a spout
 * 
 * @author zives
 *
 */
public class SpoutTask implements Runnable {
	
	IRichSpout spout;
	Queue<Runnable> queue;
	
	public SpoutTask(IRichSpout theSpout, Queue<Runnable> theQueue) {
		spout = theSpout;
		queue = theQueue;
	}

	@Override
	public void run() {
		spout.nextTuple();
		
		// Schedule ourselves again at the end of the queue
		queue.add(this);
	}

}
