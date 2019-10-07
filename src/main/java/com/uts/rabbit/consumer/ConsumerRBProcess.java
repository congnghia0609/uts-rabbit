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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since May 4, 2017
 */
public abstract class ConsumerRBProcess implements Runnable {
    private Logger logger = LoggerFactory.getLogger(ConsumerRBProcess.class);
    
    private String routingKey;
    private ConsumerRBConnect cconn;

    public ConsumerRBProcess(String routingKey) {
        if(routingKey == null || routingKey.isEmpty()){
            throw new ExceptionInInitializerError("routing_key is not empty...");
        }
        this.routingKey = routingKey;
        this.cconn = new ConsumerRBConnect(routingKey);
    }
    
    public ConsumerRBProcess(String routingKey, String amqpUrl) {
        if(routingKey == null || routingKey.isEmpty() || amqpUrl == null || amqpUrl.isEmpty()){
            throw new ExceptionInInitializerError("routing_key or suffixConfig is not empty...");
        }
        this.routingKey = routingKey;
        this.cconn = new ConsumerRBConnect(routingKey, amqpUrl);
    }

    public String getRoutingKey() {
        return routingKey;
    }
    
    @Override
    public void run(){
        try {
            System.out.println("[=========] ConsumerRBProcess["+this.routingKey+"] start...");
            Consumer consumer = new DefaultConsumer(cconn.getChannel()){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    //
                    String routingKey = envelope.getRoutingKey();
                    String contentType = properties.getContentType();
                    long deliveryTag = envelope.getDeliveryTag();

                    //String message = new String(body, "UTF-8");
                    //System.out.println(" [x] Received '" + routingKey + "':'" + message + "'");

                    // (process the message components here ...)
                    execute(body);

                    //cconn.getChannel().basicAck(deliveryTag, false);
                }
            };
            // loop that waits for message
            cconn.loopConsumer(consumer);
        } catch (Exception e) {
            logger.error("ConsumerRBProcess.run: " + e.getMessage(), e);
        }
    }
    
    public abstract void execute(byte[] data);
    
    
}
