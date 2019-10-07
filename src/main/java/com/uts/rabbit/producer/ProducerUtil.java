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

import com.uts.configer.NConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since May 3, 2017
 */
public class ProducerUtil {
    private static Logger logger = LoggerFactory.getLogger(ProducerUtil.class);
    private static final int MAX_RETRY = NConfig.getConfig().getInt("producer.maxretry", 3);
    
//    public static void sendMsg(String routingKey, byte[] msgBytes){
//        ProducerPool.getInstance(routingKey).sendMessage(msgBytes);
//    }
    
    public static int sendMsg(String routingKey, byte[] msgBytes){
        int err = -1;
        try {
            for (int i = 0; err < 0 && i < MAX_RETRY; i++) {
                err = ProducerPool.getInstance(routingKey).sendMessage(msgBytes);
            }
        } catch (Exception e) {
            err = -1;
            logger.error("ProducerUtil.sendMsg: " +  e);
        } finally {
            return err;
        }
    }
    
//    public static void sendMsgPubSub(String routingKey, byte[] msgBytes){
//        ProducerPoolPubSub.getInstance(routingKey).sendMessage(msgBytes);
//    }
}
