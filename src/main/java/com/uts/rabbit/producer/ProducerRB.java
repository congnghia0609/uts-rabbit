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
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.nio.NioParams;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since May 3, 2017
 */
public class ProducerRB {
    private Logger logger = LoggerFactory.getLogger(ProducerRB.class);
    
    private String routingKey;
    private String amqpUrl = "";
    private String exchangeName;
    private String queueName;
    private boolean durable = true;
    private boolean exclusive = false; // True: Độc quyền Queue (hạn chế cho kết nối này): (restricted to this connection)
    private boolean autoDelete = false; // True: Xóa Queue khi không còn sử dụng (server will delete Queue when no longer in use)
    private boolean mandatory = true; // True: Bắt buộc.
    private int bufferSize = 5 * 1024 * 1024; // 5 MB.
    private final String expiration = NConfig.getConfig().getString("rbproducer.expiration", "604800000"); // expiration default 7 days.
    
    private BasicProperties prop;
    private ConnectionFactory factory;
    private Connection conn;
    private Channel channel;

    public ProducerRB(String routingKey) {
        this.amqpUrl = NConfig.getConfig().getString("rbproducer.amqp_url", "");
        if(routingKey.isEmpty() || this.amqpUrl.isEmpty()){
            throw new ExceptionInInitializerError("routing_key or amqp_url is not empty...");
        }
        this.routingKey = routingKey;
        this.exchangeName = routingKey;
        this.queueName = routingKey;
        init();
        logger.info("===>>> ProducerRB [routingKey: " + this.routingKey + " | amqpUrl: " + this.amqpUrl + "]");
    }
    
    public ProducerRB(String routingKey, String amqpUrl) {
        if(routingKey.isEmpty() || amqpUrl.isEmpty()){
            throw new ExceptionInInitializerError("routing_key or amqp_url is not empty...");
        }
        this.amqpUrl = amqpUrl;
        this.routingKey = routingKey;
        this.exchangeName = routingKey;
        this.queueName = routingKey;
        init();
        logger.info("===>>> ProducerRB [routingKey: " + this.routingKey + " | amqpUrl: " + this.amqpUrl + "]");
    }
    
    private void init() {
        try {
            prop = new BasicProperties.Builder()
                        .deliveryMode(2)                    // 2: persistent mode.
                        .contentType("application/json")
                        .contentEncoding("utf-8")
                        .priority(1)
                        .expiration(expiration)
                        .build();
            factory = new ConnectionFactory();
            factory.setUri(amqpUrl);
            factory.setVirtualHost("/");
            // NIO
            factory.useNio();
            ExecutorService nioExecutor = Executors.newFixedThreadPool(16);
            NioParams niop = new NioParams();
            niop.setNbIoThreads(4);
            niop.setNioExecutor(nioExecutor);
            niop.setReadByteBufferSize(bufferSize);
            niop.setWriteByteBufferSize(bufferSize);
            factory.setNioParams(niop);
            factory.setAutomaticRecoveryEnabled(true);
            
            conn = factory.newConnection();
            channel = conn.createChannel();
            
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC, durable);
            channel.queueDeclare(queueName, durable, exclusive, autoDelete, new HashMap<String, Object>());
            channel.queueBind(queueName, exchangeName, routingKey);
            
        } catch (Exception e) {
            logger.error("ProducerRB.init: " + e.getMessage(), e);
        }
    }
    
    public boolean isOpen() {
        boolean rs = false;
        try {
            rs = conn.isOpen();
        } catch (Exception e) {
            logger.error("ProducerRB.isOpen: " + e);
        } finally {
            return rs;
        }
    }
    
    public void close(){
        if(channel != null){
            try {
                channel.close();
            } catch (Exception e) {
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                // OK
            }
        }
    }
    
//    public void sendMessage(byte[] msgBytes){
//        try {
//            channel.basicPublish(exchangeName, routingKey, mandatory, prop, msgBytes);
//        } catch (Exception e) {
//            logger.error("ProducerRB.sendMessage: " + e.getMessage(), e);
//        }
//    }
    
    public int sendMessage(byte[] msgBytes){
        int err = -1;
        try {
            channel.basicPublish(exchangeName, routingKey, mandatory, prop, msgBytes);
            err = 0;
        } catch (Exception e) {
            err = -1;
            logger.error("ProducerRB.sendMessage: " + e);
        } finally {
            return err;
        }
    }
}
