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
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import tr.com.serkanozal.dynacast.DynaCastConfigs;
import tr.com.serkanozal.dynacast.storage.DynaCastStorage;
import tr.com.serkanozal.dynacast.storage.DynaCastStorageType;

public final class DynaCastStorageProvider {

    static final AWSCredentials AWS_CREDENTIALS;
    static final boolean CLIEN_MODE_ENABLED;
    static final HazelcastInstance HZ;
    static final CacheManager CM;
    static final AmazonDynamoDB DYNAMODB;
    static final boolean DEFAULT_READ_AFTER_WRITE_SUPPORT;        
    static final long DEFAULT_READ_CAPACITY_PER_SECOND;
    static final long DEFAULT_WRITE_CAPACITY_PER_SECOND;
    
    private static final Logger LOGGER = Logger.getLogger(DynaCastDistributedStorage.class);
    @SuppressWarnings("rawtypes")
    private static final ConcurrentMap<String, DynaCastStorage> STORAGE_MAP = 
            new ConcurrentHashMap<String, DynaCastStorage>();

    static {
        try {
            Properties awsProps = getProperties("aws-credentials.properties");
            String accessKey = awsProps.getProperty("aws.accessKey");
            String secretKey = awsProps.getProperty("aws.secretKey");
            AWS_CREDENTIALS = new BasicAWSCredentials(accessKey, secretKey);
            
            //////////////////////////////////////////////////////////////
            
            Properties dynaCastProps = getProperties("dynacast.properties");
            
            //////////////////////////////////////////////////////////////
            
            boolean clientModeEnabled = false;
            String clientModeEnabledProp = 
                    (String) dynaCastProps.get(DynaCastConfigs.CLIEN_MODE_ENABLED);
            if (clientModeEnabledProp != null) {
                clientModeEnabled = Boolean.parseBoolean(clientModeEnabledProp);
            }
            CLIEN_MODE_ENABLED = clientModeEnabled;
            
            //////////////////////////////////////////////////////////////
            
            boolean readAfterWriteSupport = false;
            String readAfterWriteSupportProp = 
                    (String) dynaCastProps.get(DynaCastConfigs.READ_AFTER_WRITE_SUPPORT);
            if (readAfterWriteSupportProp != null) {
                readAfterWriteSupport = Boolean.parseBoolean(readAfterWriteSupportProp);
            }
            DEFAULT_READ_AFTER_WRITE_SUPPORT = readAfterWriteSupport;
            
            //////////////////////////////////////////////////////////////
            
            String readCapacityPerSecondProp = 
                    dynaCastProps.getProperty(DynaCastConfigs.READ_CAPACITY_PER_SECOND);
            if (readCapacityPerSecondProp != null) {
                DEFAULT_READ_CAPACITY_PER_SECOND = Long.parseLong(readCapacityPerSecondProp);
            } else {
                DEFAULT_READ_CAPACITY_PER_SECOND = 1000;
            }
            String writeCapacityPerSecondProp = 
                    dynaCastProps.getProperty(DynaCastConfigs.WRITE_CAPACITY_PER_SECOND);
            if (writeCapacityPerSecondProp != null) {
                DEFAULT_WRITE_CAPACITY_PER_SECOND = Long.parseLong(writeCapacityPerSecondProp);
            } else {
                DEFAULT_WRITE_CAPACITY_PER_SECOND = 100;
            }
            
            //////////////////////////////////////////////////////////////
            
            DYNAMODB = new AmazonDynamoDBClient(AWS_CREDENTIALS);
            
            //////////////////////////////////////////////////////////////
            
            if (CLIEN_MODE_ENABLED) {
                ClientConfig config = new ClientConfig();
                
                String clusterNameProp = 
                        dynaCastProps.getProperty(DynaCastConfigs.CLUSTER_NAME);
                if (clusterNameProp != null) {
                    config.getGroupConfig().setName(clusterNameProp);
                } else {
                    config.getGroupConfig().setName("DynaCastDistStoreCluster");
                }

                boolean clusterHostingOnAWS = false;
                String clusterHostingOnAWSProp = 
                        (String) dynaCastProps.get(DynaCastConfigs.CLUSTER_HOSTING_ON_AWS);
                if (clusterHostingOnAWSProp != null) {
                    clusterHostingOnAWS = Boolean.parseBoolean(clusterHostingOnAWSProp);
                }
                if (clusterHostingOnAWS) {
                    ClientAwsConfig awsConfig = new ClientAwsConfig();
                    awsConfig.setEnabled(true);
                    awsConfig.setAccessKey(accessKey);
                    awsConfig.setSecretKey(secretKey);
                    String clusterRegionOnAWSProp = 
                            (String) dynaCastProps.get(DynaCastConfigs.CLUSTER_REGION_ON_AWS);
                    if (clusterRegionOnAWSProp != null) {
                        awsConfig.setRegion(clusterRegionOnAWSProp);
                    }
                    config.getNetworkConfig().setAwsConfig(awsConfig);
                }
                
                HZ = HazelcastClient.newHazelcastClient(config);
                CM = HazelcastClientCachingProvider.createCachingProvider(HZ).getCacheManager();
            } else {
                Config config = new Config();
                
                String clusterNameProp = 
                        dynaCastProps.getProperty(DynaCastConfigs.CLUSTER_NAME);
                if (clusterNameProp != null) {
                    config.getGroupConfig().setName(clusterNameProp);
                } else {
                    config.getGroupConfig().setName("DynaCastDistStoreCluster");
                }

                boolean clusterHostingOnAWS = false;
                String clusterHostingOnAWSProp = 
                        (String) dynaCastProps.get(DynaCastConfigs.CLUSTER_HOSTING_ON_AWS);
                if (clusterHostingOnAWSProp != null) {
                    clusterHostingOnAWS = Boolean.parseBoolean(clusterHostingOnAWSProp);
                }
                if (clusterHostingOnAWS) {
                    JoinConfig joinConfig = config.getNetworkConfig().getJoin();
                    joinConfig.getTcpIpConfig().setEnabled(false);
                    joinConfig.getMulticastConfig().setEnabled(false);
                    AwsConfig awsConfig = joinConfig.getAwsConfig();
                    awsConfig.setEnabled(true);
                    awsConfig.setAccessKey(accessKey);
                    awsConfig.setSecretKey(secretKey);
                    String clusterRegionOnAWSProp = 
                            (String) dynaCastProps.get(DynaCastConfigs.CLUSTER_REGION_ON_AWS);
                    if (clusterRegionOnAWSProp != null) {
                        awsConfig.setRegion(clusterRegionOnAWSProp);
                    }
                }
                
                HZ = Hazelcast.newHazelcastInstance(config);
                CM = HazelcastServerCachingProvider.createCachingProvider(HZ).getCacheManager();
            }
            
        } catch (Throwable t) {
            throw new IllegalStateException("Unable to initialize distributed storage support!", t);
        }
    }

    private DynaCastStorageProvider() {
    }
    
    private static Properties getProperties(String propFileName) throws IOException {
        Properties props = new Properties();
        try {
            InputStream in = DynaCastDistributedStorage.class.getClassLoader().getResourceAsStream(propFileName);
            if (in != null) {
                props.load(in);
            } 
            props.putAll(System.getProperties());
            return props;
        } catch (IOException e) {
            LOGGER.error("Error occured while loading properties from " + "'" + propFileName + "'", e);
            throw e;
        }
    }
    
    @SuppressWarnings("resource")
    public static void discoverExistingStorages() {
        for (DistributedObject distributedObj : HZ.getDistributedObjects()) {
            if (distributedObj.getServiceName() == ICacheService.SERVICE_NAME &&
                    distributedObj.getName().startsWith("DynaCast")) {
                ICache<?, ?> cache = (ICache<?, ?>) distributedObj;
                String cacheName = cache.getName();
                String[] storageNameParts = cacheName.split("-");
                String cachePrefix = storageNameParts[0];
                String storageTypeName = storageNameParts[1];
                String storageName = cacheName.substring(cachePrefix.length() + 1 + storageTypeName.length() + 1);
                DynaCastStorageType storageType = DynaCastStorageType.valueOf(storageTypeName);
                getOrCreateStorage(storageName, storageType);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static <K, V> DynaCastStorage<K, V> getStorage(String storageName) {
        return (DynaCastStorage<K, V>) STORAGE_MAP.get(storageName);
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
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
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
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <K, V> DynaCastStorage<K, V> deleteStorage(String storageName, boolean destroy) {
        DynaCastStorage storage = STORAGE_MAP.remove(storageName);
        if (storage != null && destroy) {
            storage.destroy();
        }
        return storage;
    }

}
