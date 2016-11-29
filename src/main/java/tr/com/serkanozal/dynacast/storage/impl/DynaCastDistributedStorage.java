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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;

import org.apache.log4j.Logger;

import tr.com.serkanozal.dynacast.DynaCastConfigs;
import tr.com.serkanozal.dynacast.storage.DynaCastStorage;
import tr.com.serkanozal.dynacast.storage.DynaCastStorageType;

import com.amazonaws.AbortedException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.FastInput;
import com.esotericsoftware.kryo.io.FastOutput;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitionService;

public class DynaCastDistributedStorage<K, V> implements DynaCastStorage<K, V> {

    private static final Logger LOGGER = Logger.getLogger(DynaCastDistributedStorage.class);
    private static final AWSCredentials AWS_CREDENTIALS;
    private static final HazelcastInstance HZ;
    private static final CacheManager CM;
    private static final ThreadLocal<ReusableKryo> THREAD_LOCAL_KRYO = 
            new ThreadLocal<ReusableKryo>() {
                protected ReusableKryo initialValue() {
                    return new ReusableKryo();
                };
            };
    private static final boolean DEFAULT_READ_AFTER_WRITE_SUPPORT;        
    private static final long DEFAULT_READ_CAPACITY_PER_SECOND;
    private static final long DEFAULT_WRITE_CAPACITY_PER_SECOND;
    private static final AmazonDynamoDB DYNAMODB;
    
    static {
        try {
            Properties awsProps = getProperties("aws-credentials.properties");
            String accessKey = awsProps.getProperty("aws.accessKey");
            String secretKey = awsProps.getProperty("aws.secretKey");
            AWS_CREDENTIALS = new BasicAWSCredentials(accessKey, secretKey);
            
            //////////////////////////////////////////////////////////////
            
            Properties dynaCastProps = getProperties("dynacast.properties");
            
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
            
            Config config = new Config();
            
            String clusterNameProp = 
                    dynaCastProps.getProperty(DynaCastConfigs.CLUSTER_NAME);
            if (clusterNameProp != null) {
                config.getGroupConfig().setName(clusterNameProp);
            } else {
                config.getGroupConfig().setName("___DynaCastDistStoreCluster___");
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
        } catch (Throwable t) {
            throw new IllegalStateException("Unable to initialize distributed storage support!", t);
        }
    }
    
    private final String name;
    
    private final String dataCacheName;
    private final Cache<K, V> dataCache;
    
    private final String shardStateTableName;
    private final String dataTableName;
    
    private final Table shardStateTable;
    private final Table dataTable;
    private final AmazonDynamoDBStreamsClient dataStreams;
    private final boolean readAfterWriteSupport;        
    private final long readCapacityPerSecond;
    private final long writeCapacityPerSecond;
    
    private final ScheduledExecutorService scheduleExecutorService = 
            Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                private final ThreadFactory delegatedThreadFactory = Executors.defaultThreadFactory();
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = delegatedThreadFactory.newThread(r);
                    t.setDaemon(true);
                    return t;
                }
            });
            
    private final List<StorageMutationListener<K, V>> storageMutationListeners = 
            new CopyOnWriteArrayList<StorageMutationListener<K, V>>();   
    
    private final String uuid = UUID.randomUUID().toString();
    
    public DynaCastDistributedStorage(String name, Map<String, Object> properties) {
        this(name, properties, null);
    }
   
    public DynaCastDistributedStorage(
            String name, 
            Map<String, Object> properties, 
            StorageMutationListener<K, V> storageMutationListener) {
        this.name = name;
        
        /////////////////////////////////////////////////////////////////
        
        this.dataCacheName = "DynaCastDistStoreDataCache" + "_" + name + "___";
        this.shardStateTableName = "___" + "DynaCastDistStoreShardStateTable" + "_" + name + "___";
        this.dataTableName = "___" + "DynaCastDistStoreDataTable" + "_" + name + "___";

        /////////////////////////////////////////////////////////////////
        
        if (properties != null) {
            if (properties.get(DynaCastConfigs.READ_AFTER_WRITE_SUPPORT) != null) {
                readAfterWriteSupport = Boolean.parseBoolean(properties.get(DynaCastConfigs.READ_AFTER_WRITE_SUPPORT).toString());
            } else {
                readAfterWriteSupport = DEFAULT_READ_AFTER_WRITE_SUPPORT;
            }
            
            if (properties.get(DynaCastConfigs.READ_CAPACITY_PER_SECOND) != null) {
                readCapacityPerSecond = Long.parseLong(properties.get(DynaCastConfigs.READ_CAPACITY_PER_SECOND).toString());
            } else {
                readCapacityPerSecond = DEFAULT_READ_CAPACITY_PER_SECOND;
            }
            
            if (properties.get(DynaCastConfigs.WRITE_CAPACITY_PER_SECOND) != null) {
                writeCapacityPerSecond = Long.parseLong(properties.get(DynaCastConfigs.WRITE_CAPACITY_PER_SECOND).toString());
            } else {
                writeCapacityPerSecond = DEFAULT_WRITE_CAPACITY_PER_SECOND;
            }
        } else {
            readAfterWriteSupport = DEFAULT_READ_AFTER_WRITE_SUPPORT;
            readCapacityPerSecond = DEFAULT_READ_CAPACITY_PER_SECOND;
            writeCapacityPerSecond = DEFAULT_WRITE_CAPACITY_PER_SECOND;
        }
        
        /////////////////////////////////////////////////////////////////

        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>(dataCacheName);
        cacheConfig.setCacheLoaderFactory(new DynamodbAwareCacheLoaderFactory<K, V>(dataTableName));
        Cache<K, V> cache;
        try {
            cache = CM.createCache(dataCacheName, cacheConfig);
        } catch (CacheException e) {
            cache = CM.getCache(dataCacheName);
        }
        dataCache = cache;
        
        /////////////////////////////////////////////////////////////////
        
        dataStreams = new AmazonDynamoDBStreamsClient(AWS_CREDENTIALS);
        shardStateTable = ensureShardStateTableAvailable();
        dataTable = ensureDataTableAvailable();

        /////////////////////////////////////////////////////////////////
        
        if (storageMutationListener != null) {
            registerStorageMutationListener(storageMutationListener);
        }
    }
    
    @SuppressWarnings("serial")
    public static class DynamodbAwareCacheLoader<K, V> 
            implements CacheLoader<K, V>, Serializable {
        
        private static final AmazonDynamoDBClient DYNAMODB;
        
        static {
            try {
                Properties awsProps = getProperties("aws-credentials.properties");
                AWSCredentials awsCredentials = 
                    new BasicAWSCredentials(
                            awsProps.getProperty("aws.accessKey"), 
                            awsProps.getProperty("aws.secretKey"));
            
                DYNAMODB = new AmazonDynamoDBClient(awsCredentials);
            } catch (Throwable t) {
                throw new IllegalStateException("Unable to connect DynamoDB!", t);
            }
        }
        
        private final Table dataTable;

        public DynamodbAwareCacheLoader(String dataTableName) {
            this.dataTable = new Table(DYNAMODB, dataTableName);
        }

        @Override
        public V load(K key) throws CacheLoaderException {
            V value = null;
            byte[] keyData = serialize(key);
            Item item = 
                    dataTable.getItem(
                            new GetItemSpec().
                                    withPrimaryKey("key", keyData).
                                    withConsistentRead(true));
            if (item != null) {
                byte[] data = item.getBinary("value");
                if (data == null) {
                    value = null;
                } else {
                    value = deserialize(data);
                }
            }   
            return value;
        }

        @Override
        public Map<K, V> loadAll(Iterable<? extends K> keys)
                throws CacheLoaderException {
            throw new UnsupportedOperationException("Should not be called!");
        }
        
    }

    @SuppressWarnings("serial")
    public static class DynamodbAwareCacheLoaderFactory<K, V> 
            implements Factory<CacheLoader<K, V>>, Serializable {

        private final String dataTableName;

        public DynamodbAwareCacheLoaderFactory(String dataTableName) {
            this.dataTableName = dataTableName;
        }
        
        @Override
        public CacheLoader<K, V> create() {
            return new DynamodbAwareCacheLoader<K, V>(dataTableName);
        }
        
    }
    
    interface StorageMutationListener<K, V> {

        void onInsert(K key, V value);
        void onUpdate(K key, V oldValue, V newValue);
        void onDelete(K key);
        
    }
    
    private Table ensureShardStateTableAvailable() {
        boolean tableExist = false;
        try {
            DYNAMODB.describeTable(shardStateTableName);
            tableExist = true;
        } catch (ResourceNotFoundException e) {
        }
        
        if (!tableExist) {
            ArrayList<AttributeDefinition> attributeDefinitions = 
                    new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(
                    new AttributeDefinition().
                            withAttributeName("shardId").
                            withAttributeType("S"));

            ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(
                    new KeySchemaElement().
                            withAttributeName("shardId").
                            withKeyType(KeyType.HASH));

            CreateTableRequest createTableRequest = 
                    new CreateTableRequest().
                            withTableName(shardStateTableName).
                            withKeySchema(keySchema).
                            withAttributeDefinitions(attributeDefinitions).
                            withProvisionedThroughput(
                                    new ProvisionedThroughput().
                                            withReadCapacityUnits(readCapacityPerSecond).
                                            withWriteCapacityUnits(writeCapacityPerSecond));
            
            try {
                LOGGER.info(
                        String.format(
                                "Creating DynamoDB shard state table (%s) creation, because it is not exist", 
                                shardStateTableName));
                
                DYNAMODB.createTable(createTableRequest);
            } catch (ResourceInUseException e) { 
                LOGGER.info(
                        String.format(
                                "Ignoring DynamoDB shard state table (%s) creation, because it is already exist", 
                                shardStateTableName));
            }
        } else {
            LOGGER.info(
                    String.format(
                            "Ignoring DynamoDB shard state table (%s) creation, because it is already exist", 
                            shardStateTableName));
        }
        
        while (true) {
            DescribeTableResult describeTableResult = 
                    DYNAMODB.describeTable(shardStateTableName);
            TableDescription tableDescription = describeTableResult.getTable();
            if ("ACTIVE".equals(tableDescription.getTableStatus())) {
                break;
            }
            LOGGER.info(
                    String.format(
                            "DynamoDB shard state table (%s) is not active yet, waiting until it is active ...", 
                            shardStateTableName));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
        } 

        return new Table(DYNAMODB, shardStateTableName);
    }

    private Table ensureDataTableAvailable() {
        boolean tableExist = false;
        try {
            DYNAMODB.describeTable(dataTableName);
            tableExist = true;
        } catch (ResourceNotFoundException e) {
        }
        
        if (!tableExist) {
            ArrayList<AttributeDefinition> attributeDefinitions = 
                    new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(
                    new AttributeDefinition().
                            withAttributeName("key").
                            withAttributeType("B"));
    
            ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(
                    new KeySchemaElement().
                            withAttributeName("key").
                            withKeyType(KeyType.HASH));
    
            StreamSpecification streamSpecification = new StreamSpecification();
            streamSpecification.setStreamEnabled(true);
            streamSpecification.setStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);
    
            CreateTableRequest createTableRequest = 
                    new CreateTableRequest().
                            withTableName(dataTableName).
                            withKeySchema(keySchema).
                            withAttributeDefinitions(attributeDefinitions).
                            withStreamSpecification(streamSpecification).
                            withProvisionedThroughput(
                                    new ProvisionedThroughput().
                                            withReadCapacityUnits(readCapacityPerSecond).
                                            withWriteCapacityUnits(writeCapacityPerSecond));
            
            try {
                LOGGER.info(
                        String.format(
                                "Creating DynamoDB data table (%s) creation, because it is not exist", 
                                dataTableName));
                
                DYNAMODB.createTable(createTableRequest);
            } catch (ResourceInUseException e) { 
                LOGGER.info(
                        String.format(
                                "Ignoring DynamoDB data table (%s) creation, because it is already exist", 
                                dataTableName));
            }
        } else {
            LOGGER.info(
                    String.format(
                            "Ignoring DynamoDB data table (%s) creation, because it is already exist", 
                            dataTableName));
        }
        
        while (true) {
            DescribeTableResult describeTableResult = 
                    DYNAMODB.describeTable(dataTableName);
            TableDescription tableDescription = describeTableResult.getTable();
            if ("ACTIVE".equals(tableDescription.getTableStatus())) {
                break;
            }
            LOGGER.info(
                    String.format(
                            "DynamoDB data table (%s) is not active yet, waiting until it is active ...", 
                            dataTableName));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
        } 
        
        scheduleExecutorService.scheduleAtFixedRate(
                new StreamListener(), 
                0, 1000, TimeUnit.MILLISECONDS);
        
        return new Table(DYNAMODB, dataTableName);
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

    private class StreamListener implements Runnable {

        private final ConcurrentMap<String, String> shardSequenceNumberMap = 
                new ConcurrentHashMap<String, String>();
        private final AtomicBoolean inProgress = new AtomicBoolean();
        
        private StreamListener() {
            execute(true);
        }
        
        @Override
        public void run() {
            execute(false);
        }
        
        @SuppressWarnings("unchecked")
        private void execute(boolean initial) {
            if (inProgress.compareAndSet(false, true)) {
                try {
                    DescribeTableResult describeTableResult = 
                            DYNAMODB.describeTable(dataTableName);
                    String tableStreamArn = describeTableResult.getTable().getLatestStreamArn();
                    DescribeStreamResult describeStreamResult = 
                            dataStreams.describeStream(
                                    new DescribeStreamRequest().withStreamArn(tableStreamArn));
                    String streamArn = describeStreamResult.getStreamDescription().getStreamArn();
                    List<Shard> shards = describeStreamResult.getStreamDescription().getShards();
                    
                    PartitionService partitionService = HZ.getPartitionService();
                    
                    for (Shard shard : shards) {
                        if (Thread.currentThread().isInterrupted()) {
                            return;
                        }
                        
                        String shardId = shard.getShardId();
                        if (!partitionService.getPartition(shardId).getOwner().localMember()) {
                            // Due to possible re-partitioning, clear own cache
                            shardSequenceNumberMap.remove(shardId);
                            continue;
                        }
                        
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.info("Processing " + shardId + " from stream " + streamArn + " ...");
                        }
                        
                        String lastSequenceNumber = shardSequenceNumberMap.get(shardId);
                        if (lastSequenceNumber == null) {
                            Item shardStateItem = 
                                    shardStateTable.getItem(
                                            new GetItemSpec().
                                                    withPrimaryKey("shardId", shardId).
                                                    withConsistentRead(true));
                            if (shardStateItem != null) {
                                lastSequenceNumber = shardStateItem.getString("lastSequenceNumber");
                            }    
                        }
                        
                        String shardIterator = null;
                        if (lastSequenceNumber == null) {
                            ShardIteratorType shardIteratorType = ShardIteratorType.LATEST;
                            if (!initial) {
                                shardIteratorType = ShardIteratorType.TRIM_HORIZON;
                            }
                            GetShardIteratorRequest getShardIteratorRequest = 
                                    new GetShardIteratorRequest().
                                            withStreamArn(tableStreamArn).
                                            withShardId(shardId).
                                            withShardIteratorType(shardIteratorType);
                            GetShardIteratorResult shardIteratorResult = 
                                dataStreams.getShardIterator(getShardIteratorRequest);
                            shardIterator = shardIteratorResult.getShardIterator();
                        } else {
                            GetShardIteratorRequest getShardIteratorRequest = 
                                    new GetShardIteratorRequest().
                                            withStreamArn(tableStreamArn).
                                            withShardId(shardId).
                                            withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER).
                                            withSequenceNumber(lastSequenceNumber);
                            GetShardIteratorResult shardIteratorResult = 
                                dataStreams.getShardIterator(getShardIteratorRequest);
                            shardIterator = shardIteratorResult.getShardIterator();
                        }
                        
                        String nextItr = shardIterator;
                        while (nextItr != null) {
                            GetRecordsResult getRecordsResult = 
                                    dataStreams.getRecords(
                                            new GetRecordsRequest().withShardIterator(nextItr));
                            List<Record> records = getRecordsResult.getRecords();
                            String lastRecordSequenceNumber = null;
                            for (Record record : records) {
                                StreamRecord streamRecord = record.getDynamodb();
                                lastRecordSequenceNumber = streamRecord.getSequenceNumber();
                                String eventName = record.getEventName();
                                byte[] keyData = streamRecord.getKeys().get("key").getB().array();
                                K key = deserialize(keyData);
                                if ("INSERT".equals(eventName)) {
                                    byte[] newValueData = streamRecord.getNewImage().get("value").getB().array();
                                    String source = streamRecord.getNewImage().get("source").getS();
                                    V newValue = (V) (newValueData != null ? deserialize(newValueData) : null);
                                    dataCache.put(key, newValue);
                                    if (!source.equals(uuid)) { 
                                        for (StorageMutationListener<K, V> listener : storageMutationListeners) {
                                            listener.onInsert(key, newValue);
                                        }
                                    }    
                                } else if ("MODIFY".equals(eventName)) {
                                    byte[] oldValueData = streamRecord.getOldImage().get("value").getB().array();
                                    byte[] newValueData = streamRecord.getNewImage().get("value").getB().array();
                                    String source = streamRecord.getNewImage().get("source").getS();
                                    V oldValue = (V) (oldValueData != null ? deserialize(oldValueData) : null);
                                    V newValue = (V) (newValueData != null ? deserialize(newValueData) : null);
                                    dataCache.put(key, newValue);
                                    if (!source.equals(uuid)) { 
                                        for (StorageMutationListener<K, V> listener : storageMutationListeners) {
                                            listener.onUpdate(key, oldValue, newValue);
                                        }
                                    }    
                                } else if ("REMOVE".equals(eventName)) {
                                    dataCache.remove(key);
                                    for (StorageMutationListener<K, V> listener : storageMutationListeners) {
                                        listener.onDelete(key);
                                    }
                                } else {
                                    LOGGER.warn("Unknown event name: " + eventName);
                                }
                            }
                            
                            if (lastRecordSequenceNumber != null) {
                                shardSequenceNumberMap.put(shardId, lastRecordSequenceNumber);
                                Item shardStateItem = 
                                        new Item().
                                            withPrimaryKey("shardId", shardId).
                                            with("lastSequenceNumber", lastRecordSequenceNumber);
                                shardStateTable.putItem(shardStateItem);
                            }

                            if (records.isEmpty()) {
                                break;
                            }
                            nextItr = getRecordsResult.getNextShardIterator();
                        }
                    }
                } catch (AbortedException e) {
                    return;
                } catch (ResourceNotFoundException e) {
                    return;
                } catch (AmazonServiceException e) {
                    return;
                } catch (Throwable t) {
                    if (t instanceof InterruptedException) {
                        return;
                    }
                    LOGGER.error("Error occurred while processing stream events!", t);
                } finally {
                    inProgress.set(false);
                }
            }    
        }
        
    }

    private static class ReusableKryo extends Kryo {
        
        private static final int BUFFER_SIZE = 4096;
        
        private final FastOutput output = new FastOutput(BUFFER_SIZE);

        private byte[] encode(Object obj) {
            output.clear();
            writeClassAndObject(output, obj);
            return output.toBytes();
        }
        
        private Object decode(byte[] data) {
            return readClassAndObject(new FastInput(data));
        }
        
    }
    
    private static <T> byte[] serialize(T obj) {
        return THREAD_LOCAL_KRYO.get().encode(obj);
    }
    
    @SuppressWarnings("unchecked")
    private static <T> T deserialize(byte[] data) {
        return (T) THREAD_LOCAL_KRYO.get().decode(data);
    }
    
    void registerStorageMutationListener(StorageMutationListener<K, V> storageMutationListener) {
        storageMutationListeners.add(storageMutationListener);
    }
    
    void deregisterStorageMutationListener(StorageMutationListener<K, V> storageMutationListener) {
        storageMutationListeners.remove(storageMutationListener);
    }
    
    @Override
    public String getName() {
        return name;
    }

    @Override
    public DynaCastStorageType getType() {
        return DynaCastStorageType.DISTRIBUTED;
    }

    @Override
    public V get(K key) {
        V value = (V) dataCache.get(key);
        if (value != null) {
            return value;
        }
        byte[] keyData = serialize(key);
        Item item = 
                dataTable.getItem(
                        new GetItemSpec().
                                withPrimaryKey("key", keyData).
                                withConsistentRead(true));
        if (item == null) {
            value = null;
        } else {
            byte[] data = item.getBinary("value");
            if (data == null) {
                value = null;
            } else {
                value = deserialize(data);
            }
        }    
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    String.format("Value %s has been retrieved from distributed storage with key %s", key, value));
        }
        
        return value;
    }
    
    @Override
    public V refresh(K key) {
        return get(key);
    }

    @Override
    public void put(K key, V value) {
        if (readAfterWriteSupport) {
            dataCache.remove(key);
        }
        
        if (value == null) {
            remove(key);
        } else {
            byte[] keyData = serialize(key);
            byte[] valueData = serialize(value);
            Item item = 
                    new Item().
                        withPrimaryKey("key", keyData).
                        withBinary("value", valueData).
                        with("source", uuid);
            dataTable.putItem(item);
            
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        String.format("Value %s has been put into distributed storage with key %s", key, value));
            }
        }    
    }
    
    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        if (readAfterWriteSupport) {
            dataCache.remove(key);
        }
        
        byte[] keyData = serialize(key);
        boolean replaced = false;
        if (oldValue == null && newValue != null) {
            byte[] newValueData = serialize(newValue);
            Item item = 
                    new Item().
                        withPrimaryKey("key", keyData).
                        withBinary("value", newValueData).
                        with("source", uuid);
            try {
                dataTable.putItem(item, new Expected("key").notExist());
                replaced = true;
            } catch (ConditionalCheckFailedException e) {
            }
        } else if (oldValue != null && newValue == null) {
            byte[] oldValueData = serialize(oldValue);
            try {
                dataTable.deleteItem("key", keyData, new Expected("value").eq(oldValueData));
                replaced = true;
            } catch (ConditionalCheckFailedException e) {
            }
        } else if (oldValue != null && newValue != null) {
            byte[] oldValueData = serialize(oldValue);
            byte[] newValueData = serialize(newValue);
            Item item = 
                    new Item().
                        withPrimaryKey("key", keyData).
                        withBinary("value", newValueData).
                        with("source", uuid);
            try {
                dataTable.putItem(item, new Expected("value").eq(oldValueData));
                replaced = true;
            } catch (ConditionalCheckFailedException e) {
            }
        }    
        
        if (replaced && LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    String.format("Old value %s has been replaced with new value %s " + 
                                  "assigned to key %s", oldValue, newValue, key));
        }
        
        return replaced;
    }

    @Override
    public void remove(K key) {
        if (readAfterWriteSupport) {
            dataCache.remove(key);
        }
        
        byte[] keyData = serialize(key);
        dataTable.deleteItem("key", keyData);
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    String.format("Value has been removed from distributed storage with key %s", key));
        }
    }
    
    @Override
    public void clear() {
        if (readAfterWriteSupport) {
            dataCache.removeAll();
        }
        
        ItemCollection<ScanOutcome> items = dataTable.scan();
        IteratorSupport<Item, ScanOutcome> itemsIter = items.iterator();
        while (itemsIter.hasNext()) {
            Item item = itemsIter.next();
            dataTable.deleteItem("key", item.get("key"));
        }
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Distributed storage has been cleared");
        }
    }

    @Override
    public void destroy() {
        scheduleExecutorService.shutdownNow();
        try {
            scheduleExecutorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
        
        try {
            CM.destroyCache(dataCacheName);
        } catch (IllegalStateException e) {
        }
        
        try {
            dataTable.delete();
            while (true) {
                dataTable.describe();
                LOGGER.info(
                        String.format(
                                "DynamoDB data table (%s) is still exist and delete is in progress, " + 
                                "waiting until it has been destroyed ...", 
                                dataTableName));
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                }
            } 
        } catch (ResourceNotFoundException e) {
        }  
        
        try {
            shardStateTable.delete();
            while (true) {
                shardStateTable.describe();
                LOGGER.info(
                        String.format(
                                "DynamoDB shard state table (%s) is still exist and delete is in progress, " + 
                                "waiting until it has been destroyed ...", 
                                dataTableName));
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                }
            } 
        } catch (ResourceNotFoundException e) {
        }  

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Distributed storage has been destroyed");
        }
    }
    
}
