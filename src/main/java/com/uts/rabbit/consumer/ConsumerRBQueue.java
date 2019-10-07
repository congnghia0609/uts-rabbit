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

package com.uts.rabbit.consumer;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since May 4, 2017
 */
public class ConsumerRBQueue {
    private Logger logger = LoggerFactory.getLogger(ConsumerRBQueue.class);
    private List<ConsumerRBProcess> consumers = new LinkedList<ConsumerRBProcess>();

    public ConsumerRBQueue() {
    }
    
    public void add(ConsumerRBProcess process){
        consumers.add(process);
    }
    
    public int start(){
        try {
            for(ConsumerRBProcess consume : consumers){
                startProcess(consume);
            }
        } catch (Exception e) {
            logger.error("ConsumerRBQueue.start: " + e.getMessage(), e);
            System.out.println("ConsumerRBQueue start error !!!");
            return -1;
        }
        System.out.println("ConsumerRBQueue start successfully !!!");
        return 0;
    }
    
    private int startProcess(ConsumerRBProcess process){
        try {
            ExecutorService executor = Executors.newFixedThreadPool(1);
            executor.execute(process);
        } catch (Exception e) {
            logger.error("ConsumerRBQueue.startProcess fail...", e);
            return -1;
        }
        return 0;
    }
}
