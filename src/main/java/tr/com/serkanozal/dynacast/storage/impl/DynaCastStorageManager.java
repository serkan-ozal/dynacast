/*
 * Copyright (c) 2016, Serkan OZAL, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tr.com.serkanozal.dynacast.storage.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.cache.CacheManager;

import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import tr.com.serkanozal.dynacast.DynaCastConfigs;
import tr.com.serkanozal.dynacast.storage.DynaCastStorage;
import tr.com.serkanozal.dynacast.storage.DynaCastStorageType;

@SuppressWarnings({ "resource", "rawtypes" })
public final class DynaCastStorageManager {

    static final Properties DYNACAST_PROPS;
    static final AWSCredentials AWS_CREDENTIALS;
    static final boolean USE_PUBLIC_IPS_ON_AWS;
    static final boolean CLIEN_MODE_ENABLED;
    static final HazelcastInstance HZ;
    static final CacheManager CM;
    static final AmazonDynamoDB DYNAMODB;
    static final boolean DEFAULT_READ_AFTER_WRITE_SUPPORT;        
    static final long DEFAULT_READ_CAPACITY_PER_SECOND;
    static final long DEFAULT_WRITE_CAPACITY_PER_SECOND;
    static final int DEFAULT_LOCAL_CACHE_CAPACITY;
    static final int DEFAULT_DISTRIBUTED_CACHE_CAPACITY;
    
    private static final Logger LOGGER = Logger.getLogger(DynaCastDistributedStorage.class);
    private static final ConcurrentMap<String, DynaCastStorage> STORAGE_MAP = 
            new ConcurrentHashMap<String, DynaCastStorage>();

    static {
        long start = System.currentTimeMillis();
        
        try {
            DYNACAST_PROPS = getProperties("dynacast.properties", true);
            
            //////////////////////////////////////////////////////////////
            
            Properties awsProps = getProperties("aws-credentials.properties", false);
            String accessKey = awsProps.getProperty("aws.accessKey");
            String secretKey = awsProps.getProperty("aws.secretKey");
            AWS_CREDENTIALS = new BasicAWSCredentials(accessKey, secretKey);
            
            //////////////////////////////////////////////////////////////
            
            boolean usePublicIPsOnAws = false;
            String usePublicIPsOnAwsProp = 
                    DYNACAST_PROPS.getProperty(DynaCastConfigs.USE_PUBLIC_IPS_ON_AWS);
            if (usePublicIPsOnAwsProp != null) {
                usePublicIPsOnAws = Boolean.parseBoolean(usePublicIPsOnAwsProp);
            }
            USE_PUBLIC_IPS_ON_AWS = usePublicIPsOnAws;
            
            //////////////////////////////////////////////////////////////
            
            boolean clientModeEnabled = false;
            String clientModeEnabledProp = 
                    DYNACAST_PROPS.getProperty(DynaCastConfigs.CLIEN_MODE_ENABLED);
            if (clientModeEnabledProp != null) {
                clientModeEnabled = Boolean.parseBoolean(clientModeEnabledProp);
            }
            CLIEN_MODE_ENABLED = clientModeEnabled;
            
            //////////////////////////////////////////////////////////////
            
            boolean readAfterWriteSupport = false;
            String readAfterWriteSupportProp = 
                    DYNACAST_PROPS.getProperty(DynaCastConfigs.READ_AFTER_WRITE_SUPPORT);
            if (readAfterWriteSupportProp != null) {
                readAfterWriteSupport = Boolean.parseBoolean(readAfterWriteSupportProp);
            }
            DEFAULT_READ_AFTER_WRITE_SUPPORT = readAfterWriteSupport;
            
            //////////////////////////////////////////////////////////////
            
            String readCapacityPerSecondProp = 
                    DYNACAST_PROPS.getProperty(DynaCastConfigs.READ_CAPACITY_PER_SECOND);
            if (readCapacityPerSecondProp != null) {
                DEFAULT_READ_CAPACITY_PER_SECOND = Long.parseLong(readCapacityPerSecondProp);
            } else {
                DEFAULT_READ_CAPACITY_PER_SECOND = 1000;
            }
            String writeCapacityPerSecondProp = 
                    DYNACAST_PROPS.getProperty(DynaCastConfigs.WRITE_CAPACITY_PER_SECOND);
            if (writeCapacityPerSecondProp != null) {
                DEFAULT_WRITE_CAPACITY_PER_SECOND = Long.parseLong(writeCapacityPerSecondProp);
            } else {
                DEFAULT_WRITE_CAPACITY_PER_SECOND = 100;
            }
            
            //////////////////////////////////////////////////////////////
            
            String localCacheCapacityProp = 
                    DYNACAST_PROPS.getProperty(DynaCastConfigs.LOCAL_CACHE_CAPACITY);
            if (localCacheCapacityProp != null) {
                DEFAULT_LOCAL_CACHE_CAPACITY = Integer.parseInt(localCacheCapacityProp);
            } else {
                DEFAULT_LOCAL_CACHE_CAPACITY = -1;
            }
            String distributedCacheCapacityProp = 
                    DYNACAST_PROPS.getProperty(DynaCastConfigs.DISTRIBUTED_CACHE_CAPACITY);
            if (distributedCacheCapacityProp != null) {
                DEFAULT_DISTRIBUTED_CACHE_CAPACITY = Integer.parseInt(distributedCacheCapacityProp);
            } else {
                DEFAULT_DISTRIBUTED_CACHE_CAPACITY = -1;
            }
            
            //////////////////////////////////////////////////////////////
            
            DYNAMODB = new AmazonDynamoDBClient(AWS_CREDENTIALS);
            
            //////////////////////////////////////////////////////////////
            
            if (CLIEN_MODE_ENABLED) {
                ClientConfig config = new ClientConfig();
                
                String clusterNameProp = 
                        DYNACAST_PROPS.getProperty(DynaCastConfigs.CLUSTER_NAME);
                if (clusterNameProp != null) {
                    config.getGroupConfig().setName(clusterNameProp);
                } else {
                    config.getGroupConfig().setName("DynaCastDistStoreCluster");
                }

                boolean clusterHostingOnAWS = false;
                String clusterHostingOnAWSProp = 
                        DYNACAST_PROPS.getProperty(DynaCastConfigs.CLUSTER_HOSTING_ON_AWS);
                if (clusterHostingOnAWSProp != null) {
                    clusterHostingOnAWS = Boolean.parseBoolean(clusterHostingOnAWSProp);
                }
                if (clusterHostingOnAWS) {
                    ClientAwsConfig awsConfig = new ClientAwsConfig();
                    awsConfig.setEnabled(true);
                    awsConfig.setAccessKey(accessKey);
                    awsConfig.setSecretKey(secretKey);
                    String clusterRegionOnAWSProp = 
                            DYNACAST_PROPS.getProperty(DynaCastConfigs.CLUSTER_REGION_ON_AWS);
                    if (clusterRegionOnAWSProp != null) {
                        awsConfig.setRegion(clusterRegionOnAWSProp);
                    }
                    if (USE_PUBLIC_IPS_ON_AWS) {
                        awsConfig.setInsideAws(false);
                    } else {
                        awsConfig.setInsideAws(true);
                    }
                    config.getNetworkConfig().setAwsConfig(awsConfig);
                }
                
                HZ = HazelcastClient.newHazelcastClient(config);
                CM = HazelcastClientCachingProvider.createCachingProvider(HZ).getCacheManager();
            } else {
                Config config = new Config();
                
                String clusterNameProp = 
                        DYNACAST_PROPS.getProperty(DynaCastConfigs.CLUSTER_NAME);
                if (clusterNameProp != null) {
                    config.getGroupConfig().setName(clusterNameProp);
                } else {
                    config.getGroupConfig().setName("DynaCastDistStoreCluster");
                }

                boolean clusterHostingOnAWS = false;
                String clusterHostingOnAWSProp = 
                        DYNACAST_PROPS.getProperty(DynaCastConfigs.CLUSTER_HOSTING_ON_AWS);
                if (clusterHostingOnAWSProp != null) {
                    clusterHostingOnAWS = Boolean.parseBoolean(clusterHostingOnAWSProp);
                }
                if (clusterHostingOnAWS) {
                    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
                    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
                    
                    AwsConfig awsConfig = config.getNetworkConfig().getJoin().getAwsConfig();
                    awsConfig.setEnabled(true);
                    awsConfig.setAccessKey(accessKey);
                    awsConfig.setSecretKey(secretKey);
                    String clusterRegionOnAWSProp = 
                            DYNACAST_PROPS.getProperty(DynaCastConfigs.CLUSTER_REGION_ON_AWS);
                    if (clusterRegionOnAWSProp != null) {
                        awsConfig.setRegion(clusterRegionOnAWSProp);
                    }
                    
                    /*
                    Map<String, Comparable> disoveryProperties = new HashMap<String, Comparable>();
                    disoveryProperties.put(JCloudsProperties.PROVIDER.key(), "aws-ec2");
                    disoveryProperties.put(JCloudsProperties.IDENTITY.key(), accessKey);
                    disoveryProperties.put(JCloudsProperties.CREDENTIAL.key(), secretKey);
                    String clusterRegionOnAWSProp = 
                            DYNACAST_PROPS.getProperty(DynaCastConfigs.CLUSTER_REGION_ON_AWS);
                    if (clusterRegionOnAWSProp != null) {
                        disoveryProperties.put(JCloudsProperties.REGIONS.key(), clusterRegionOnAWSProp);
                    }
                    DiscoveryStrategyConfig discoveryStrategyConfig = 
                            new DiscoveryStrategyConfig(
                                    JCloudsDiscoveryStrategy.class.getName(),
                                    disoveryProperties);
                    DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
                    discoveryConfig.addDiscoveryStrategyConfig(discoveryStrategyConfig);
                    
                    config.setProperty(GroupProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
                    if (USE_PUBLIC_IPS_ON_AWS) {
                        config.setProperty(GroupProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED.getName(), "true");
                    }   
                    */ 
                }
                
                HZ = Hazelcast.newHazelcastInstance(config);
                CM = HazelcastServerCachingProvider.createCachingProvider(HZ).getCacheManager();
            }
            
        } catch (Throwable t) {
            throw new IllegalStateException("Unable to initialize distributed storage support!", t);
        }
        
        //////////////////////////////////////////////////////////////
        
        Map<String, String> props = DynaCastStorageManager.getProperties();
        if (!props.isEmpty()) {
            for (Map.Entry<String, String> entry : props.entrySet()) {
                String propName = entry.getKey();
                String propValue = entry.getValue();
                if (propName.startsWith("dynacast.storage.")) {
                    String storageName = propName.substring("dynacast.storage.".length());
                    DynaCastStorageType storageType = DynaCastStorageType.valueOf(propValue);
                    LOGGER.info(
                            String.format(
                                    "Creating configured storage '%s' as '%s' typed in the local ...", 
                                    storageName, storageType.name()));
                    getOrCreateStorage(storageName, storageType);
                }
            }
        }  
        
        //////////////////////////////////////////////////////////////
        
        if (!CLIEN_MODE_ENABLED) {
            for (DistributedObject distributedObj : HZ.getDistributedObjects()) {
                if (distributedObj.getServiceName().equals(ICacheService.SERVICE_NAME) &&
                        distributedObj.getName().startsWith("DynaCast")) {
                    ICache<?, ?> cache = (ICache<?, ?>) distributedObj;
                    String cacheName = cache.getName();
                    String[] storageNameParts = cacheName.split("-");
                    String cachePrefix = storageNameParts[0];
                    String storageTypeName = storageNameParts[1];
                    String storageName = cacheName.substring(cachePrefix.length() + 1 + storageTypeName.length() + 1);
                    DynaCastStorageType storageType = DynaCastStorageType.valueOf(storageTypeName);
                    LOGGER.info(
                            String.format(
                                    "Creating globally existing storage '%s' as '%s' typed in the local ...", 
                                    storageName, storageType.name()));
                    getOrCreateStorage(storageName, storageType);
                }
            }
        }
        
        //////////////////////////////////////////////////////////////
        
        HZ.addDistributedObjectListener(new DistributedObjectListener() {
            @Override
            public void distributedObjectCreated(DistributedObjectEvent event) {
                if (!CLIEN_MODE_ENABLED) {
                    String objectName = event.getObjectName().toString();
                    String serviceName = event.getServiceName();
                    if (serviceName.equals(ICacheService.SERVICE_NAME) && 
                            objectName.startsWith(HazelcastCacheManager.CACHE_MANAGER_PREFIX + "DynaCast")) {
                        String cacheName = objectName.substring(HazelcastCacheManager.CACHE_MANAGER_PREFIX.length());
                        String[] storageNameParts = cacheName.split("-");
                        String cachePrefix = storageNameParts[0];
                        String storageTypeName = storageNameParts[1];
                        String storageName = cacheName.substring(cachePrefix.length() + 1 + storageTypeName.length() + 1);
                        DynaCastStorageType storageType = DynaCastStorageType.valueOf(storageTypeName);
                        LOGGER.info(
                                String.format(
                                        "Creating newly created storage '%s' as '%s' typed in the local ...", 
                                        storageName, storageType.name()));
                        getOrCreateStorage(storageName, storageType);
                    }
                }    
            }
            @Override
            public void distributedObjectDestroyed(DistributedObjectEvent event) {
                String objectName = event.getObjectName().toString();
                String serviceName = event.getServiceName();
                if (serviceName.equals(ICacheService.SERVICE_NAME) && 
                        objectName.startsWith(HazelcastCacheManager.CACHE_MANAGER_PREFIX + "DynaCast")) {
                    String cacheName = objectName.substring(HazelcastCacheManager.CACHE_MANAGER_PREFIX.length());
                    String[] storageNameParts = cacheName.split("-");
                    String cachePrefix = storageNameParts[0];
                    String storageTypeName = storageNameParts[1];
                    String storageName = cacheName.substring(cachePrefix.length() + 1 + storageTypeName.length() + 1);
                    LOGGER.info(
                            String.format(
                                    "Deleting globally destroyed storage '%s' as '%s' typed in the local ...", 
                                    storageName, storageTypeName));
                    deleteStorage(storageName, true);
                }
            }
        });
        
        LOGGER.info(DynaCastStorageManager.class.getSimpleName() + 
                    " has initialized in " + (System.currentTimeMillis() - start) + " milliseconds");
    }

    private DynaCastStorageManager() {
    }
    
    private static Properties getProperties(String propFileName, boolean requiresDynaCastPrefix) throws IOException {
        Properties props = new Properties();
        try {
            InputStream in = DynaCastDistributedStorage.class.getClassLoader().getResourceAsStream(propFileName);
            if (in != null) {
                props.load(in);
            } 
            Properties sysProps = System.getProperties();
            for (String sysPropName : sysProps.stringPropertyNames()) {
                if (requiresDynaCastPrefix) {
                    if (sysPropName.startsWith("dynacast")) {
                        props.put(sysPropName, sysProps.getProperty(sysPropName));
                    } 
                } else {
                    props.put(sysPropName, sysProps.getProperty(sysPropName));
                }
            }
            return props;
        } catch (IOException e) {
            LOGGER.error("Error occured while loading properties from " + "'" + propFileName + "'", e);
            throw e;
        }
    }
    
    @SuppressWarnings({ "unchecked" })
    public static Map<String, String> getProperties() {
        return (Map) Collections.unmodifiableMap(DYNACAST_PROPS);
    }
    
    public static boolean isClienModeEnabled() {
        return CLIEN_MODE_ENABLED;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> DynaCastStorage<K, V> getStorage(String storageName) {
        return (DynaCastStorage<K, V>) STORAGE_MAP.get(storageName);
    }
    
    public static Map<String, DynaCastStorage> getStorages() {
        return Collections.unmodifiableMap(STORAGE_MAP);
    }
    
    public static <K, V> DynaCastStorage<K, V> createStorage(
            String storageName, 
            DynaCastStorageType storageType) {
        return createStorage(storageName, storageType, null);
    }
    
    public static <K, V> DynaCastStorage<K, V> createStorage(
            String storageName, 
            DynaCastStorageType storageType,
            Map<String, Object> properties) {
        switch(storageType) {
            case DISTRIBUTED:
                return new DynaCastDistributedStorage<K, V>(storageName, storageType, properties);
            case TIERED:
                return new DynaCastTieredStorage<K, V>(storageName, storageType, properties);
            default:
                throw new IllegalArgumentException("Unknow storage type: " + storageType + 
                        "! Valid values are " + Arrays.asList(DynaCastStorageType.values()));
        }
    }
    
    public static <K, V> DynaCastStorage<K, V> getOrCreateStorage(
            String storageName, 
            DynaCastStorageType storageType) {
        return getOrCreateStorage(storageName, storageType, null);
    }
    
    @SuppressWarnings({ "unchecked" })
    public static <K, V> DynaCastStorage<K, V> getOrCreateStorage(
            String storageName, 
            DynaCastStorageType storageType,
            Map<String, Object> properties) {
        DynaCastStorage storage = STORAGE_MAP.get(storageName);
        if (storage == null) {
            synchronized (STORAGE_MAP) {
                storage = STORAGE_MAP.get(storageName);
                if (storage == null) {
                    storage = createStorage(storageName, storageType, properties);
                    STORAGE_MAP.put(storageName, storage);
                }
            }
        }
        return storage;
    }
    
    @SuppressWarnings({ "unchecked" })
    public static <K, V> DynaCastStorage<K, V> deleteStorage(String storageName, boolean destroy) {
        DynaCastStorage storage = STORAGE_MAP.remove(storageName);
        if (storage != null && destroy) {
            storage.destroy();
        }
        return storage;
    }

}
