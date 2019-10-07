/*
 * Copyright 2017 nghiatc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uts.rabbit.producer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since May 3, 2017
 */
public class ProducerPoolPubSub {
    private Logger logger = LoggerFactory.getLogger(ProducerPoolPubSub.class);
    
    private static Map<String, ProducerRBPubSub> mapProducers = new ConcurrentHashMap<>();
    private static Lock lock = new ReentrantLock();

	public static ProducerRBPubSub getInstance(String routingKey) {
        if(routingKey == null || routingKey.isEmpty()){
            return null;
        }
        ProducerRBPubSub instance = mapProducers.containsKey(routingKey) ? mapProducers.get(routingKey) : null;
		if(instance == null) {
			lock.lock();
			try {
                instance = mapProducers.containsKey(routingKey) ? mapProducers.get(routingKey) : null;
				if(instance == null) {
					instance = new ProducerRBPubSub(routingKey);
                    mapProducers.put(routingKey, instance);
				}
			} finally {
				lock.unlock();
			}
		}
		return instance;
	}
    
    public static ProducerRBPubSub getInstance(String routingKey, String amqpUrl) {
        if(routingKey == null || routingKey.isEmpty() || amqpUrl == null || amqpUrl.isEmpty()){
            return null;
        }
        ProducerRBPubSub instance = mapProducers.containsKey(routingKey) ? mapProducers.get(routingKey) : null;
		if(instance == null) {
			lock.lock();
			try {
                instance = mapProducers.containsKey(routingKey) ? mapProducers.get(routingKey) : null;
				if(instance == null) {
					instance = new ProducerRBPubSub(routingKey, amqpUrl);
                    mapProducers.put(routingKey, instance);
				}
			} finally {
				lock.unlock();
			}
		}
		return instance;
	}
    
    
    
}
