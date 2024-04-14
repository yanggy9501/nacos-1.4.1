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

package com.alibaba.nacos.client.naming.beat;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.api.naming.NamingResponseCode;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.net.NamingProxy;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Beat reactor.
 *
 * @author harold
 */
public class BeatReactor implements Closeable {

    private final ScheduledExecutorService executorService;

    private final NamingProxy serverProxy;

    private boolean lightBeatEnabled = false;

    // 服务key 及 BeatInfo 的缓存映射
    public final Map<String, BeatInfo> dom2Beat = new ConcurrentHashMap<String, BeatInfo>();

    public BeatReactor(NamingProxy serverProxy) {
        this(serverProxy, UtilAndComs.DEFAULT_CLIENT_BEAT_THREAD_COUNT);
    }

    public BeatReactor(NamingProxy serverProxy, int threadCount) {
        this.serverProxy = serverProxy;
        // 心跳任务线程池
        this.executorService = new ScheduledThreadPoolExecutor(threadCount, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("com.alibaba.nacos.naming.beat.sender");
                return thread;
            }
        });
    }

    /**
     * Add beat information. 添加 心跳信息
     *
     * @param serviceName service name
     * @param beatInfo    beat information
     */
    public void addBeatInfo(String serviceName, BeatInfo beatInfo) {
        NAMING_LOGGER.info("[BEAT] adding beat: {} to beat map.", beatInfo);
        String key = buildKey(serviceName, beatInfo.getIp(), beatInfo.getPort());
        BeatInfo existBeat = null;
        //fix #1733
        // 添加一次是添加开始，添加两次是停止心跳
        if ((existBeat = dom2Beat.remove(key)) != null) {
            existBeat.setStopped(true);
        }
        // 将需要心跳的服务key及 BeatInfo 做缓存映射
        dom2Beat.put(key, beatInfo);
        /**
         * 对每个服务 添加心跳任务 默认5s 一次心跳（这里先添加的是一个延迟任务,延迟5s执行）
         * 心跳如果发现 nacos server 端不存在服务资源则自动重新注册
         */
        executorService.schedule(new BeatTask(beatInfo), beatInfo.getPeriod(), TimeUnit.MILLISECONDS);
        MetricsMonitor.getDom2BeatSizeMonitor().set(dom2Beat.size());
    }

    /**
     * Remove beat information. 移除心跳
     *
     * @param serviceName service name
     * @param ip          ip of beat information
     * @param port        port of beat information
     */
    public void removeBeatInfo(String serviceName, String ip, int port) {
        NAMING_LOGGER.info("[BEAT] removing beat: {}:{}:{} from beat map.", serviceName, ip, port);
        // 从 dom2Beat 缓存长移除 BeatInfo
        BeatInfo beatInfo = dom2Beat.remove(buildKey(serviceName, ip, port));
        if (beatInfo == null) {
            return;
        }
        // 设置停止
        beatInfo.setStopped(true);
        MetricsMonitor.getDom2BeatSizeMonitor().set(dom2Beat.size());
    }

    /**
     * Build new beat information.
     *
     * @param instance instance
     * @return new beat information
     */
    public BeatInfo buildBeatInfo(Instance instance) {
        return buildBeatInfo(instance.getServiceName(), instance);
    }

    /**
     * Build new beat information.
     *
     * @param groupedServiceName service name with group name, format: ${groupName}@@${serviceName}
     * @param instance           instance
     * @return new beat information
     */
    public BeatInfo buildBeatInfo(String groupedServiceName, Instance instance) {
        BeatInfo beatInfo = new BeatInfo();
        beatInfo.setServiceName(groupedServiceName);
        beatInfo.setIp(instance.getIp());
        beatInfo.setPort(instance.getPort());
        beatInfo.setCluster(instance.getClusterName());
        beatInfo.setWeight(instance.getWeight());
        beatInfo.setMetadata(instance.getMetadata());
        beatInfo.setScheduled(false);
        beatInfo.setPeriod(instance.getInstanceHeartBeatInterval());
        return beatInfo;
    }

    public String buildKey(String serviceName, String ip, int port) {
        return serviceName + Constants.NAMING_INSTANCE_ID_SPLITTER + ip + Constants.NAMING_INSTANCE_ID_SPLITTER + port;
    }

    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executorService, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }

    class BeatTask implements Runnable {

        BeatInfo beatInfo;

        public BeatTask(BeatInfo beatInfo) {
            this.beatInfo = beatInfo;
        }

        /**
         * 定时心跳 心跳如果发现 nacos server端无法找到服务资源(比如:nacos server重启丢失了服务信息)，则自动重新注册
         */
        @Override
        public void run() {
            // stopped 代表是否需要停止心跳
            if (beatInfo.isStopped()) {
                return;
            }
            long nextTime = beatInfo.getPeriod();
            try {
                // 发送心跳请求获取心跳响应
                JsonNode result = serverProxy.sendBeat(beatInfo, BeatReactor.this.lightBeatEnabled);
                long interval = result.get("clientBeatInterval").asLong();
                boolean lightBeatEnabled = false;
                if (result.has(CommonParams.LIGHT_BEAT_ENABLED)) {
                    lightBeatEnabled = result.get(CommonParams.LIGHT_BEAT_ENABLED).asBoolean();
                }
                BeatReactor.this.lightBeatEnabled = lightBeatEnabled;
                if (interval > 0) {
                    nextTime = interval;
                }
                int code = NamingResponseCode.OK;
                if (result.has(CommonParams.CODE)) {
                    code = result.get(CommonParams.CODE).asInt();
                }
                /**
                 * 如果心跳发现服务资源不存在，则自动重新注册
                 */
                if (code == NamingResponseCode.RESOURCE_NOT_FOUND) {
                    Instance instance = new Instance();
                    instance.setPort(beatInfo.getPort());
                    instance.setIp(beatInfo.getIp());
                    instance.setWeight(beatInfo.getWeight());
                    instance.setMetadata(beatInfo.getMetadata());
                    instance.setClusterName(beatInfo.getCluster());
                    instance.setServiceName(beatInfo.getServiceName());
                    instance.setInstanceId(instance.getInstanceId());
                    instance.setEphemeral(true);
                    try {
                        // 重新注册
                        serverProxy.registerService(beatInfo.getServiceName(),
                            NamingUtils.getGroupName(beatInfo.getServiceName()), instance);
                    } catch (Exception ignore) {
                    }
                }
            } catch (NacosException ex) {
                NAMING_LOGGER.error("[CLIENT-BEAT] failed to send beat: {}, code: {}, msg: {}",
                    JacksonUtils.toJson(beatInfo), ex.getErrCode(), ex.getErrMsg());

            }
            // 再次添加一个延迟任务 （造成类似定时效果）
            executorService.schedule(new BeatTask(beatInfo), nextTime, TimeUnit.MILLISECONDS);
        }
    }
}
