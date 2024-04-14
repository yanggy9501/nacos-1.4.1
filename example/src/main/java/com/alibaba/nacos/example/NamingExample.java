/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.example;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.AbstractEventListener;
import com.alibaba.nacos.api.naming.listener.Event;
import com.alibaba.nacos.api.naming.listener.NamingEvent;

import java.util.Properties;
import java.util.concurrent.*;

/**
 * Nacos naming example.
 *
 * @author nkorange
 */
public class NamingExample {
    /*
        nacso client 完成服务的注册，发现，取消注册，订阅请按照：step1, step2, step3, step4依次来看
        集群模式下Distro协议数据同步
        1、集群节点的互相发现：com.alibaba.nacos.core.cluster.ServerMemberManager.ServerMemberManager
        2、启动后开始分区数据定时校验和启动加载全量快照数据：com.alibaba.nacos.core.distributed.distro.DistroProtocol.DistroProtocol
        3、运行过程中的增量数据同步：节点数据发生变更后进行同步
           比如：有服务注册后 在nacos server端会发生数据同步，入口：com.alibaba.nacos.naming.controllers.InstanceController.register
     */
    public static void main(String[] args) throws NacosException {

        Properties properties = new Properties();
/*
        properties.setProperty("serverAddr", System.getProperty("serverAddr"));
        properties.setProperty("namespace", System.getProperty("namespace"));
*/
        // 在控制台创建好命名空间（public 默认存在）
        properties.setProperty("serverAddr", "127.0.0.1:8848");// nacos 服务注册中心地址
        properties.setProperty("namespace", "public");// 命名空间

        /**
         * 获取命名服务对象: NacosNamingService 每个命名空间对应一个命名服务对象
         */
        NamingService naming = NamingFactory.createNamingService(properties);

        /**
         * （step1:）注册服务实例  registerInstance方法的定义有很多
         */
        naming.registerInstance("nacos.test.1", "192.168.200.10", 8888);
        naming.registerInstance("nacos.test.1", "192.168.200.10", 7777);
        naming.registerInstance("nacos.test.2", "DEFAULT_GROUP", "192.168.200.10", 9999);

        /**
         * 获取/发现服务实例
         * (step2 )
         */
        System.out.println(naming.getAllInstances("nacos.test.1"));

        /**
         * 服务注销(下线)
         * （step3）
         */
        naming.deregisterInstance("nacos.test.1", "192.168.200.10", 7777);

        System.out.println(naming.getAllInstances("nacos.test.1"));

        Executor executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("test-thread");
                return thread;
            }
        });

        /**
         * 服务订阅
         * （step4）
         */
        naming.subscribe("nacos.test.3", new AbstractEventListener() {

            //EventListener onEvent is sync to handle, If process too low in onEvent, maybe block other onEvent callback.
            //So you can override getExecutor() to async handle event.
            @Override
            public Executor getExecutor() {
                return executor;
            }

            @Override
            public void onEvent(Event event) {
                System.out.println(((NamingEvent) event).getServiceName());
                System.out.println(((NamingEvent) event).getInstances());
            }
        });
    }
}
