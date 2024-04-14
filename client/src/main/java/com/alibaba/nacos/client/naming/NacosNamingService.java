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

package com.alibaba.nacos.client.naming;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.selector.AbstractSelector;
import com.alibaba.nacos.client.naming.beat.BeatInfo;
import com.alibaba.nacos.client.naming.beat.BeatReactor;
import com.alibaba.nacos.client.naming.core.Balancer;
import com.alibaba.nacos.client.naming.core.HostReactor;
import com.alibaba.nacos.client.naming.net.NamingProxy;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.InitUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.client.utils.ValidatorUtils;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Nacos Naming Service. 一个命名空间对应一个 NacosNamingService 实例
 *
 * @author nkorange
 */
@SuppressWarnings("PMD.ServiceOrDaoClassShouldEndWithImplRule")
public class NacosNamingService implements NamingService {

    /**
     * Each Naming service should have different namespace. 表明每个命名空间对应一个 命名服务对象
     */
    private String namespace;

    private String endpoint;
    // nacos server 地址列表
    private String serverList;

    private String cacheDir;

    private String logName;

    private HostReactor hostReactor;
    // 心跳处理器
    private BeatReactor beatReactor;

    private NamingProxy serverProxy;

    public NacosNamingService(String serverList) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, serverList);
        init(properties);
    }

    /**
     * 构造函数入口
     *
     * @param properties
     * @throws NacosException
     */
    public NacosNamingService(Properties properties) throws NacosException {
        /**
         * 初始化操作
         */
        init(properties);
    }

    /**
     * 根据 Properties进行初始化, Properties 中至少包含： serverAddr=127.0.0.1:8848 namespace=demo
     *
     * @param properties
     * @throws NacosException
     */
    private void init(Properties properties) throws NacosException {
        ValidatorUtils.checkInitParam(properties);
        // 获取命名空间 比如： demo
        this.namespace = InitUtils.initNamespaceForNaming(properties);
        // 序列化采用 Jackson
        InitUtils.initSerialization();
        // 根据properties 获取 nacos server 地址列表  this.serverList 有了
        initServerAddr(properties);
        InitUtils.initWebRootContext(properties);
        initCacheDir();// 初始化本地缓存所在的文件目录
        initLogName(properties);
        /**
         *  创建了三个比较重要的对象
         *  serverProxy=NamingProxy：http client 封装，nacos 客户端最终都是通过 serverProxy 和 nacos 服务端交互的
         *  beatReactor=BeatReactor：心跳检查，健康状态的检查
         *  hostReactor=HostReactor：
         */
        this.serverProxy = new NamingProxy(this.namespace, this.endpoint, this.serverList, properties);
        // 心跳反应器（定时任务线程池）
        this.beatReactor = new BeatReactor(this.serverProxy, initClientBeatThreadCount(properties));

        this.hostReactor = new HostReactor(this.serverProxy, beatReactor, this.cacheDir, isLoadCacheAtStart(properties),
            isPushEmptyProtect(properties), initPollingThreadCount(properties));
    }

    private int initClientBeatThreadCount(Properties properties) {
        if (properties == null) {
            return UtilAndComs.DEFAULT_CLIENT_BEAT_THREAD_COUNT;
        }

        return ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.NAMING_CLIENT_BEAT_THREAD_COUNT),
            UtilAndComs.DEFAULT_CLIENT_BEAT_THREAD_COUNT);
    }

    private int initPollingThreadCount(Properties properties) {
        if (properties == null) {

            return UtilAndComs.DEFAULT_POLLING_THREAD_COUNT;
        }

        return ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.NAMING_POLLING_THREAD_COUNT),
            UtilAndComs.DEFAULT_POLLING_THREAD_COUNT);
    }

    private boolean isLoadCacheAtStart(Properties properties) {
        boolean loadCacheAtStart = false;
        if (properties != null && StringUtils
            .isNotEmpty(properties.getProperty(PropertyKeyConst.NAMING_LOAD_CACHE_AT_START))) {
            loadCacheAtStart = ConvertUtils
                .toBoolean(properties.getProperty(PropertyKeyConst.NAMING_LOAD_CACHE_AT_START));
        }

        return loadCacheAtStart;
    }

    private boolean isPushEmptyProtect(Properties properties) {
        boolean pushEmptyProtection = false;
        if (properties != null && StringUtils
            .isNotEmpty(properties.getProperty(PropertyKeyConst.NAMING_PUSH_EMPTY_PROTECTION))) {
            pushEmptyProtection = ConvertUtils
                .toBoolean(properties.getProperty(PropertyKeyConst.NAMING_PUSH_EMPTY_PROTECTION));
        }
        return pushEmptyProtection;
    }

    private void initServerAddr(Properties properties) {
        serverList = properties.getProperty(PropertyKeyConst.SERVER_ADDR);
        endpoint = InitUtils.initEndpoint(properties);
        if (StringUtils.isNotEmpty(endpoint)) {
            serverList = "";
        }
    }

    private void initLogName(Properties properties) {
        logName = System.getProperty(UtilAndComs.NACOS_NAMING_LOG_NAME);
        if (StringUtils.isEmpty(logName)) {

            if (properties != null && StringUtils
                .isNotEmpty(properties.getProperty(UtilAndComs.NACOS_NAMING_LOG_NAME))) {
                logName = properties.getProperty(UtilAndComs.NACOS_NAMING_LOG_NAME);
            } else {
                logName = "naming.log";
            }
        }
    }

    private void initCacheDir() {
        String jmSnapshotPath = System.getProperty("JM.SNAPSHOT.PATH");
        if (!StringUtils.isBlank(jmSnapshotPath)) {
            cacheDir =
                jmSnapshotPath + File.separator + "nacos" + File.separator + "naming" + File.separator + namespace;
        } else {
            cacheDir = System.getProperty("user.home") + File.separator + "nacos" + File.separator + "naming"
                + File.separator + namespace;
        }
    }

    /**
     * 服务注册
     *
     * @param serviceName name of service
     * @param ip          instance ip
     * @param port        instance port
     * @throws NacosException
     */
    @Override
    public void registerInstance(String serviceName, String ip, int port) throws NacosException {
        registerInstance(serviceName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }

    @Override
    public void registerInstance(String serviceName, String groupName, String ip, int port) throws NacosException {
        registerInstance(serviceName, groupName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }

    @Override
    public void registerInstance(String serviceName, String ip, int port, String clusterName) throws NacosException {
        registerInstance(serviceName, Constants.DEFAULT_GROUP, ip, port, clusterName);
    }

    /**
     * 完整的服务注册方法
     *
     * @param serviceName name of service           服务名称
     * @param groupName   group of service          分组名称
     * @param ip          instance ip               实例ip
     * @param port        instance port             实例端口
     * @param clusterName instance cluster name     集群名称
     * @throws NacosException
     */
    @Override
    public void registerInstance(String serviceName, String groupName, String ip, int port, String clusterName)
        throws NacosException {
        // 创建一个服务的一个实例对象, 填充相关属性
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        instance.setWeight(1.0);
        instance.setClusterName(clusterName);
        // 注册服务实例
        registerInstance(serviceName, groupName, instance);
    }

    @Override
    public void registerInstance(String serviceName, Instance instance) throws NacosException {
        registerInstance(serviceName, Constants.DEFAULT_GROUP, instance);
    }

    @Override
    public void registerInstance(String serviceName, String groupName, Instance instance) throws NacosException {
        NamingUtils.checkInstanceIsLegal(instance);
        // 将groupName和serviceName组装到一起变成：groupName@@serviceName
        String groupedServiceName = NamingUtils.getGroupedName(serviceName, groupName);
        if (instance.isEphemeral()) {
            // 创建心跳消息，并添加心跳
            BeatInfo beatInfo = beatReactor.buildBeatInfo(groupedServiceName, instance);
            /**
             * 对每个服务 添加心跳任务默认 5s 一次心跳
             * 心跳如果发现 nacos server 端不存在服务资源则自动重新注册
             */
            beatReactor.addBeatInfo(groupedServiceName, beatInfo);
        }
        // 远程通信，注册该服务实例
        serverProxy.registerService(groupedServiceName, groupName, instance);
    }

    /**
     * 下线服务 （取消注册）
     *
     * @param serviceName name of service
     * @param ip          instance ip
     * @param port        instance port
     * @throws NacosException
     */
    @Override
    public void deregisterInstance(String serviceName, String ip, int port) throws NacosException {
        deregisterInstance(serviceName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }

    @Override
    public void deregisterInstance(String serviceName, String groupName, String ip, int port) throws NacosException {
        deregisterInstance(serviceName, groupName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }

    @Override
    public void deregisterInstance(String serviceName, String ip, int port, String clusterName) throws NacosException {
        deregisterInstance(serviceName, Constants.DEFAULT_GROUP, ip, port, clusterName);
    }

    /**
     * 下线服务 （取消注册）
     *
     * @param serviceName name of service            服务名称
     * @param groupName   group of service           分组名称
     * @param ip          instance ip                实例ip
     * @param port        instance port              实例端口
     * @param clusterName instance cluster name      实例所在集群名称
     * @throws NacosException
     */
    @Override
    public void deregisterInstance(String serviceName, String groupName, String ip, int port, String clusterName)
        throws NacosException {
        // 创建实例对象
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        instance.setClusterName(clusterName);
        // 取消注册
        deregisterInstance(serviceName, groupName, instance);
    }

    @Override
    public void deregisterInstance(String serviceName, Instance instance) throws NacosException {
        deregisterInstance(serviceName, Constants.DEFAULT_GROUP, instance);
    }

    @Override
    public void deregisterInstance(String serviceName, String groupName, Instance instance) throws NacosException {
        if (instance.isEphemeral()) {
            // 取消服务心跳
            beatReactor.removeBeatInfo(NamingUtils.getGroupedName(serviceName, groupName), instance.getIp(),
                instance.getPort());
        }
        // 取消服务注册
        serverProxy.deregisterService(NamingUtils.getGroupedName(serviceName, groupName), instance);
    }

    /**
     * 获取服务实例
     *
     * @param serviceName name of service
     * @return
     * @throws NacosException
     */
    @Override
    public List<Instance> getAllInstances(String serviceName) throws NacosException {
        return getAllInstances(serviceName, new ArrayList<String>());
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName) throws NacosException {
        return getAllInstances(serviceName, groupName, new ArrayList<String>());
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, boolean subscribe) throws NacosException {
        return getAllInstances(serviceName, new ArrayList<String>(), subscribe);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, boolean subscribe)
        throws NacosException {
        return getAllInstances(serviceName, groupName, new ArrayList<String>(), subscribe);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, List<String> clusters) throws NacosException {
        return getAllInstances(serviceName, clusters, true);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, List<String> clusters)
        throws NacosException {
        return getAllInstances(serviceName, groupName, clusters, true);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, List<String> clusters, boolean subscribe)
        throws NacosException {
        return getAllInstances(serviceName, Constants.DEFAULT_GROUP, clusters, subscribe);
    }

    /**
     * 获取服务下的实例列表信息
     *
     * @param serviceName name of service           服务名称
     * @param groupName   group of service          分组名称
     * @param clusters    list of cluster           集群信息
     * @param subscribe   if subscribe the service  是否订阅（缓存）
     * @return
     * @throws NacosException
     */
    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, List<String> clusters,
        boolean subscribe) throws NacosException {
        ServiceInfo serviceInfo;
        // 订阅从缓存中获取，缓存定期更新；非订阅则从nacos服务中获取
        if (subscribe) {
            // 获取 服务信息 走缓存
            serviceInfo = hostReactor.getServiceInfo(NamingUtils.getGroupedName(serviceName, groupName),
                StringUtils.join(clusters, ","));
        } else {
            // 直接从 nacos server 获取服务信息 不走缓存
            serviceInfo = hostReactor
                .getServiceInfoDirectlyFromServer(NamingUtils.getGroupedName(serviceName, groupName),
                    StringUtils.join(clusters, ","));
        }
        List<Instance> list;
        if (serviceInfo == null || CollectionUtils.isEmpty(list = serviceInfo.getHosts())) {
            return new ArrayList<Instance>();
        }
        return list;
    }

    @Override
    public List<Instance> selectInstances(String serviceName, boolean healthy) throws NacosException {
        return selectInstances(serviceName, new ArrayList<String>(), healthy);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, boolean healthy) throws NacosException {
        return selectInstances(serviceName, groupName, healthy, true);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, boolean healthy, boolean subscribe)
        throws NacosException {
        return selectInstances(serviceName, new ArrayList<String>(), healthy, subscribe);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, boolean healthy, boolean subscribe)
        throws NacosException {
        return selectInstances(serviceName, groupName, new ArrayList<String>(), healthy, subscribe);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, List<String> clusters, boolean healthy)
        throws NacosException {
        return selectInstances(serviceName, clusters, healthy, true);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, List<String> clusters, boolean healthy)
        throws NacosException {
        return selectInstances(serviceName, groupName, clusters, healthy, true);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, List<String> clusters, boolean healthy, boolean subscribe)
        throws NacosException {
        return selectInstances(serviceName, Constants.DEFAULT_GROUP, clusters, healthy, subscribe);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, List<String> clusters, boolean healthy,
        boolean subscribe) throws NacosException {

        ServiceInfo serviceInfo;
        if (subscribe) {
            serviceInfo = hostReactor.getServiceInfo(NamingUtils.getGroupedName(serviceName, groupName),
                StringUtils.join(clusters, ","));
        } else {
            serviceInfo = hostReactor
                .getServiceInfoDirectlyFromServer(NamingUtils.getGroupedName(serviceName, groupName),
                    StringUtils.join(clusters, ","));
        }
        return selectInstances(serviceInfo, healthy);
    }

    private List<Instance> selectInstances(ServiceInfo serviceInfo, boolean healthy) {
        List<Instance> list;
        if (serviceInfo == null || CollectionUtils.isEmpty(list = serviceInfo.getHosts())) {
            return new ArrayList<Instance>();
        }

        Iterator<Instance> iterator = list.iterator();
        while (iterator.hasNext()) {
            Instance instance = iterator.next();
            if (healthy != instance.isHealthy() || !instance.isEnabled() || instance.getWeight() <= 0) {
                iterator.remove();
            }
        }

        return list;
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName) throws NacosException {
        return selectOneHealthyInstance(serviceName, new ArrayList<String>());
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName) throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, true);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, boolean subscribe) throws NacosException {
        return selectOneHealthyInstance(serviceName, new ArrayList<String>(), subscribe);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, boolean subscribe)
        throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, new ArrayList<String>(), subscribe);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, List<String> clusters) throws NacosException {
        return selectOneHealthyInstance(serviceName, clusters, true);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, List<String> clusters)
        throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, clusters, true);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, List<String> clusters, boolean subscribe)
        throws NacosException {
        return selectOneHealthyInstance(serviceName, Constants.DEFAULT_GROUP, clusters, subscribe);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, List<String> clusters,
        boolean subscribe) throws NacosException {

        if (subscribe) {
            return Balancer.RandomByWeight.selectHost(hostReactor
                .getServiceInfo(NamingUtils.getGroupedName(serviceName, groupName),
                    StringUtils.join(clusters, ",")));
        } else {
            return Balancer.RandomByWeight.selectHost(hostReactor
                .getServiceInfoDirectlyFromServer(NamingUtils.getGroupedName(serviceName, groupName),
                    StringUtils.join(clusters, ",")));
        }
    }

    /**
     * 服务订阅
     *
     * @param serviceName name of service
     * @param listener    event listener
     * @throws NacosException
     */
    @Override
    public void subscribe(String serviceName, EventListener listener) throws NacosException {
        subscribe(serviceName, new ArrayList<String>(), listener);
    }

    @Override
    public void subscribe(String serviceName, String groupName, EventListener listener) throws NacosException {
        subscribe(serviceName, groupName, new ArrayList<String>(), listener);
    }

    @Override
    public void subscribe(String serviceName, List<String> clusters, EventListener listener) throws NacosException {
        subscribe(serviceName, Constants.DEFAULT_GROUP, clusters, listener);
    }

    /**
     * 服务订阅
     *
     * @param serviceName name of service              服务名称
     * @param groupName   group of service             分组名称
     * @param clusters    list of cluster              集群参数
     * @param listener    event listener               监听器
     * @throws NacosException
     */
    @Override
    public void subscribe(String serviceName, String groupName, List<String> clusters, EventListener listener)
        throws NacosException {
        hostReactor.subscribe(NamingUtils.getGroupedName(serviceName, groupName), StringUtils.join(clusters, ","),
            listener);
    }

    @Override
    public void unsubscribe(String serviceName, EventListener listener) throws NacosException {
        unsubscribe(serviceName, new ArrayList<String>(), listener);
    }

    @Override
    public void unsubscribe(String serviceName, String groupName, EventListener listener) throws NacosException {
        unsubscribe(serviceName, groupName, new ArrayList<String>(), listener);
    }

    @Override
    public void unsubscribe(String serviceName, List<String> clusters, EventListener listener) throws NacosException {
        unsubscribe(serviceName, Constants.DEFAULT_GROUP, clusters, listener);
    }

    @Override
    public void unsubscribe(String serviceName, String groupName, List<String> clusters, EventListener listener)
        throws NacosException {
        hostReactor.unSubscribe(NamingUtils.getGroupedName(serviceName, groupName), StringUtils.join(clusters, ","),
            listener);
    }

    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize) throws NacosException {
        return serverProxy.getServiceList(pageNo, pageSize, Constants.DEFAULT_GROUP);
    }

    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, String groupName) throws NacosException {
        return getServicesOfServer(pageNo, pageSize, groupName, null);
    }

    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, AbstractSelector selector)
        throws NacosException {
        return getServicesOfServer(pageNo, pageSize, Constants.DEFAULT_GROUP, selector);
    }

    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, String groupName, AbstractSelector selector)
        throws NacosException {
        return serverProxy.getServiceList(pageNo, pageSize, groupName, selector);
    }

    @Override
    public List<ServiceInfo> getSubscribeServices() {
        return hostReactor.getSubscribeServices();
    }

    @Override
    public String getServerStatus() {
        return serverProxy.serverHealthy() ? "UP" : "DOWN";
    }

    public BeatReactor getBeatReactor() {
        return beatReactor;
    }

    @Override
    public void shutDown() throws NacosException {
        beatReactor.shutdown();
        hostReactor.shutdown();
        serverProxy.shutdown();
    }
}
