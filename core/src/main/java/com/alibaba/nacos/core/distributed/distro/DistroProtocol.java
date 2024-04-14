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

package com.alibaba.nacos.core.distributed.distro;

import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.component.DistroCallback;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataStorage;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.core.distributed.distro.task.DistroTaskEngineHolder;
import com.alibaba.nacos.core.distributed.distro.task.delay.DistroDelayTask;
import com.alibaba.nacos.core.distributed.distro.task.load.DistroLoadDataTask;
import com.alibaba.nacos.core.distributed.distro.task.verify.DistroVerifyTask;
import com.alibaba.nacos.core.utils.GlobalExecutor;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.springframework.stereotype.Component;

/**
 * Distro protocol.
 *
 * @author xiweng.yy
 */
@Component
public class DistroProtocol {

    private final ServerMemberManager memberManager;

    private final DistroComponentHolder distroComponentHolder;

    private final DistroTaskEngineHolder distroTaskEngineHolder;

    private final DistroConfig distroConfig;

    private volatile boolean isInitialized = false;

    public DistroProtocol(ServerMemberManager memberManager, DistroComponentHolder distroComponentHolder,
            DistroTaskEngineHolder distroTaskEngineHolder, DistroConfig distroConfig) {
        this.memberManager = memberManager;
        this.distroComponentHolder = distroComponentHolder;
        this.distroTaskEngineHolder = distroTaskEngineHolder;
        this.distroConfig = distroConfig;
        /**
         * 启动 DistroTask
         */
        startDistroTask();
    }

    private void startDistroTask() {
        // 单机模式下返回
        if (EnvUtil.getStandaloneMode()) {
            isInitialized = true;
            return;
        }
        /*
        启动 startVerifyTask，做分区数据同步校验
        当前 nacos 节点把自己负责的 service 的 key 和实例的签名数据发送给其他 nacos 节点。
        其他节点收到 key 和签名后，对比本地数据，筛选出需要删除和更新的服务
         */
        startVerifyTask();
        //启动 DistroLoadDataTask，用于启动时全量加载数据
        startLoadTask();
    }

    private void startLoadTask() {
        DistroCallback loadCallback = new DistroCallback() {
            @Override
            public void onSuccess() {
                isInitialized = true;
            }

            @Override
            public void onFailed(Throwable throwable) {
                isInitialized = false;
            }
        };
        /*
        启动加载远程快照数据到本地
         */
        GlobalExecutor.submitLoadDataTask(
                new DistroLoadDataTask(memberManager, distroComponentHolder, distroConfig, loadCallback));
    }

    private void startVerifyTask() {
        /*
            定时对分区数据进行同步校验 ，默认是5s
            重点关注：DistroVerifyTask
         */
        GlobalExecutor.schedulePartitionDataTimedSync(new DistroVerifyTask(memberManager, distroComponentHolder),
                distroConfig.getVerifyIntervalMillis());
    }

    public boolean isInitialized() {
        return isInitialized;
    }

    /**
     * Start to sync by configured delay.
     *
     * @param distroKey distro key of sync data
     * @param action    the action of data operation
     */
    public void sync(DistroKey distroKey, DataOperation action) {
        sync(distroKey, action, distroConfig.getSyncDelayMillis());
    }

    /**
     * Start to sync data to all remote server.
     *  开始和远程其他nacos server 同步数据
     * @param distroKey distro key of sync data
     * @param action    the action of data operation
     */
    public void sync(DistroKey distroKey, DataOperation action, long delay) {
        /*
            memberManager.allMembersWithoutSelf() 获取集群除自己以外的成员信息
         */
        for (Member each : memberManager.allMembersWithoutSelf()) {
            // 创建 DistroKey
            DistroKey distroKeyWithTarget = new DistroKey(distroKey.getResourceKey(), distroKey.getResourceType(),
                    each.getAddress());
            // 创建 DistroDelayTask 任务
            DistroDelayTask distroDelayTask = new DistroDelayTask(distroKeyWithTarget, action, delay);
            /*
                添加延迟任务 执行   查看 addTask 将任务添加到 tasks 队列中

                getDelayTaskExecuteEngine返回的是：DistroDelayTaskExecuteEngine
                 addTask 在其父类： NacosDelayTaskExecuteEngine
                 下一个入口：NacosDelayTaskExecuteEngine 构造
             */
            distroTaskEngineHolder.getDelayTaskExecuteEngine().addTask(distroKeyWithTarget, distroDelayTask);
            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.debug("[DISTRO-SCHEDULE] {} to {}", distroKey, each.getAddress());
            }
        }
    }

    /**
     * Query data from specified server.
     *
     * @param distroKey data key
     * @return data
     */
    public DistroData queryFromRemote(DistroKey distroKey) {
        if (null == distroKey.getTargetServer()) {
            Loggers.DISTRO.warn("[DISTRO] Can't query data from empty server");
            return null;
        }
        String resourceType = distroKey.getResourceType();
        DistroTransportAgent transportAgent = distroComponentHolder.findTransportAgent(resourceType);
        if (null == transportAgent) {
            Loggers.DISTRO.warn("[DISTRO] Can't find transport agent for key {}", resourceType);
            return null;
        }
        return transportAgent.getData(distroKey, distroKey.getTargetServer());
    }

    /**
     * Receive synced distro data, find processor to process.
     *
     * @param distroData Received data
     * @return true if handle receive data successfully, otherwise false
     */
    public boolean onReceive(DistroData distroData) {
        String resourceType = distroData.getDistroKey().getResourceType();
        DistroDataProcessor dataProcessor = distroComponentHolder.findDataProcessor(resourceType);
        if (null == dataProcessor) {
            Loggers.DISTRO.warn("[DISTRO] Can't find data process for received data {}", resourceType);
            return false;
        }
        // 处理增量同步数据
        return dataProcessor.processData(distroData);
    }

    /**
     * Receive verify data, find processor to process.
     *
     * @param distroData verify data
     * @return true if verify data successfully, otherwise false
     */
    public boolean onVerify(DistroData distroData) {
        String resourceType = distroData.getDistroKey().getResourceType();
        DistroDataProcessor dataProcessor = distroComponentHolder.findDataProcessor(resourceType);
        if (null == dataProcessor) {
            Loggers.DISTRO.warn("[DISTRO] Can't find verify data process for received data {}", resourceType);
            return false;
        }
        // 处理verify 数据
        return dataProcessor.processVerifyData(distroData);
    }

    /**
     * Query data of input distro key.
     *
     * @param distroKey key of data
     * @return data
     */
    public DistroData onQuery(DistroKey distroKey) {
        String resourceType = distroKey.getResourceType();
        DistroDataStorage distroDataStorage = distroComponentHolder.findDataStorage(resourceType);
        if (null == distroDataStorage) {
            Loggers.DISTRO.warn("[DISTRO] Can't find data storage for received key {}", resourceType);
            return new DistroData(distroKey, new byte[0]);
        }
        return distroDataStorage.getDistroData(distroKey);
    }

    /**
     * Query all datum snapshot.
     *
     * @param type datum type
     * @return all datum snapshot
     */
    public DistroData onSnapshot(String type) {
        DistroDataStorage distroDataStorage = distroComponentHolder.findDataStorage(type);
        if (null == distroDataStorage) {
            Loggers.DISTRO.warn("[DISTRO] Can't find data storage for received key {}", type);
            return new DistroData(new DistroKey("snapshot", type), new byte[0]);
        }
        return distroDataStorage.getDatumSnapshot();
    }
}
