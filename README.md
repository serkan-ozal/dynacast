# dynacast
**Hazelcast** cached and synched **AWS DynamoDB**

1. What is DynaCast?
==============
Simply it is for caching **AWS DynamoDB** with **Hazelcast** and keeping them eventually consistent. **DynaCast** is a very simple caching library based on **Hazelcast** (`Cast` comes from here) on top of **AWS DynamoDB** (`Dyna` comes from here) with very basic caching functionalities (get, put, replace, remove) to be used as distributed or tiered (local + distributed). 

**DynaCast** caches data in-memory via **Hazelcast** as distributed internally and persists data into **AWS DynamoDB**. Under the hood, cache data in **Hazelcast** is stored as *eventually consistent* with **AWS DynamoDB** by receiving mutation events (ordered by the shard/partition) from **AWS DynamoDB Streams** (See [here](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html) for more details about **AWS DynamoDB Streams**). 

Demo application to show how to use **DynaCast** is available [here](https://github.com/serkan-ozal/dynacast-demo).

2. Installation
==============
In your `pom.xml`, you must add repository and dependency for **DynaCast**. 
You can change `dynacast.version` to any existing **DynaCast** library version.
Latest version of **DynaCast** is `1.0.1-SNAPSHOT`.

``` xml
...
<properties>
    ...
    <samba.version>1.0.1-SNAPSHOT</samba.version>
    ...
</properties>
...
<dependencies>
    ...
	<dependency>
		<groupId>tr.com.serkanozal</groupId>
		<artifactId>dynacast</artifactId>
		<version>${dynacast.version}</version>
	</dependency>
	...
</dependencies>
...
<repositories>
	...
	<repository>
		<id>serkanozal-maven-repository</id>
		<url>https://github.com/serkan-ozal/maven-repository/raw/master/</url>
	</repository>
	...
</repositories>
...
```

3. Configuration
==============

3.1. AWS Credentials
--------------
* **`aws.accessKey:`** Your AWS access key
* **`aws.secretKey:`** Your AWS secret key

These properties can be specified as system property or can be given from **`aws-credentials.properties`** configuration file.

3.2. DynaCast Configurations
--------------
* **`dynacast.readCapacityPerSecond`**: Configures expected maxiumum read capacity to provision required throughput from AWS DynamoDB. Default value is `1000`.
* **`dynacast.writeCapacityPerSecond`**: Configures expected maxiumum write capacity to provision required throughput from AWS DynamoDB. Default value is `100`.
* **`dynacast.clusterName`**: Configures name of the cluster. If you want to isolate each cluster from other on the same environment, you must configure cluster name via this property. Default value is `___DynaCastDistStoreCluster___`.
* **`dynacast.readYourWriteSupport`**: Enables *read-your-write* consistency (See [here](https://en.wikipedia.org/wiki/Consistency_model#Read-your-writes_Consistency) and [here](http://www.dbms2.com/2010/05/01/ryw-read-your-writes-consistency) for more details) support. Default value is `false`.
* **`dynacast.clusterHostingOnAWS`**: Enables AWS based discovery support when there are multiple nodes with DynaCast. By this property enabled, Hazelcast instances, which are used by DynaCast internally for caching data, discovers each other (IPs of other nodes) through AWS API and forms a cluster. Default value is `false`.
* **`dynacast.clusterRegionOnAWS`**: Configures name of the AWS region where the application, which contains and uses DynaCast, is deployed. Default value is `us-east-1`. So if the application is deployed into another region, this property must be configured accordingly, otherwise nodes cannot discover each other.
* **`dynacast.clientModeEnabled`**: Enables client mode. DynaCast clients need DynaCast servers to connect. Default value is `false`. See [Client-Server Architecture(https://github.com/serkan-ozal/dynacast#4-dynacast-configurations) section for more details
* **`dynacast.usePublicIPsOnAWS`**: By this property enabled, clients connects to servers on AWS through their public IPs (not private). Note that this property is only supported in *client* mode on *AWS*. So this means that, to use this property, ,`dynacast.clusterRegionOnAWS` and `dynacast.clientModeEnabled` properties must be `true`. Default value is `false`.
* **`dynacast.localCacheCapacity`**: Configures capacity of the local cachen when `TIERED` storage is used. However, due to sampling based eviction algorithm and relaxed size check (which is not atomic with put operation itself because of performance reasons) on concurrent accesses, actual size might be higher than capacity. So from this perspective, capacity is a just hint for the local cache, not a strict limit. Default value is `-1` which means unbounded capacity.
* **`dynacast.distributedCacheCapacity`**: Configures capacity of the distributed cache. However, due to sampling based eviction algorithm and global cache size estimation (global cache size is approximated from the partition size with standard deviation based statistical algorithms), actual size might be higher than capacity (See [here](http://docs.hazelcast.org/docs/3.7/manual/html-single/index.html#jcache-eviction) for more details). So from this perspective, capacity is a just hint for the distributed cache, not a strict limit. Default value is `-1` which means unbounded capacity.

These properties can be specified as system property or can be given from **`dynacast.properties`** configuration file.

4. Client-Server Architecture

5. Usage
==============
The contact points for the user are `tr.com.serkanozal.dynacast.DynaCast` and `tr.com.serkanozal.dynacast.storage.DynaCastStorage` classes. 

5.1. DynaCast
--------------
`tr.com.serkanozal.dynacast.DynaCast` is the conact point for accessing, creating and deleting storages.
Here are the exposed API methods through `tr.com.serkanozal.dynacast.DynaCast`:

* **`<K, V> DynaCastStorage<K, V> getStorage(String storageName)`**: Gets the storage associated with given name. Returns `null` if it is not exist.

* **`<K, V> DynaCastStorage<K, V> getOrCreateStorage(String storageName, DynaCastStorageType storageType`**: Gets (if it is exist) or creates (if it is not exist) the storage associated with given name.

* **`<K, V> DynaCastStorage<K, V> getOrCreateStorage(String storageName, DynaCastStorageType storageType, Map<String, Object> properties)`**: Gets (if it is exist) or creates (if it is not exist) the storage associated with given name by configuring via give `properties`. Per storage based supports properties are `dynacast.readCapacityPerSecond`, `dynacast.writeCapacityPerSecond`, `dynacast.readYourWriteSupport`, `dynacast.localCacheCapacity` and `dynacast.distributedCacheCapacity`. See [DynaCast Configurations](https://github.com/serkan-ozal/dynacast#32-dynacast-configurations) section for more details.

* **`<K, V> DynaCastStorage<K, V> deleteStorage(String storageName, boolean destroy)`**: Deletes the storage associated with given name. Also destroys it if `destroy` parameter is `true`.

5.2. DynaCastStorage
--------------
`tr.com.serkanozal.dynacast.storage.DynaCastStorage` stores and provides values by their associated keys. 

There are two types of storages at the moment:
* `DISRIBUTED`: Persists entities to the AWS DynamoDB for highly-scalable and high-performance accesses. Also entities are cached in the memory via Hazelcast as distributed. When an entry is requested, it is first looked up in the cache. If it is found, returns to the caller, otherwise loads it from AWS DynamoDB, puts it into cache (if exists) and then returns to the caller. In here, Hazelcast based cache is kept *eventually consistent* with AWS DynamoDB via AWS DynamoDB Streams (See [here](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html) for more details).
* `TIERED`: Keeps entities on local and remote storages. While updating/removing an entity, it is updated/removed on both of local and remote storages. In addition, while getting entity, at first it is looked up on local storage. If it is available, it is directly retrieved from local storage, otherwise it is requested from remote storage. In this mode, `DynaCastStorage` instance supports *eventual consistency* model. This means that if an entity is updated/removed in/from remote storage by someone, local storage is updated eventually. In this context, there is *monotonic read consistency* but no *linearizability*. See [here](https://en.wikipedia.org/wiki/Consistency_model) and [here](https://aphyr.com/posts/313-strong-consistency-models) for more details.

5.3. Sample Usage
--------------
Here is the sample usage of **DynaCast**:

``` java
        DynaCastStorage<Integer, String> storage = 
                DynaCast.getOrCreateStorage("MyStorage", DynaCastStorageType.TIERED);
        
        ///////////////////////////////////////////////////////////////////////
        
        System.out.println("Clearing storage initially ...");
        
        storage.clear();
        
        ///////////////////////////////////////////////////////////////////////
        
        System.out.println("================================");
        for (int i = 0; i < 10; i++) {
            System.out.println("\t- [" + i + "]: " + storage.get(i));
        }
        System.out.println("================================\n");
        
        ///////////////////////////////////////////////////////////////////////
        
        for (int i = 0; i < 10; i++) {
            System.out.println(String.format("Put key: %d, value: %s ...", i, "value-" + i));
            storage.put(i, "value-" + i);
        }
        
        ///////////////////////////////////////////////////////////////////////
        
        System.out.println("================================");
        for (int i = 0; i < 10; i++) {
            System.out.println("\t- [" + i + "]: " + storage.get(i));
        }
        System.out.println("================================\n");

        ///////////////////////////////////////////////////////////////////////
        
        for (int i = 0; i < 10; i++) {
            System.out.println(String.format("Remove key: %d ...", i));
            storage.remove(i);
        }
        
        ///////////////////////////////////////////////////////////////////////
        
        System.out.println("================================");
        for (int i = 0; i < 10; i++) {
            System.out.println("\t- [" + i + "]: " + storage.get(i));
        }
        System.out.println("================================\n");
        
        ///////////////////////////////////////////////////////////////////////
        
        System.out.println("Destroying storage ...");
        
        storage.destroy();
```
