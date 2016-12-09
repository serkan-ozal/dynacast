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
package tr.com.serkanozal.dynacast;

public interface DynaCastConfigs {

    String READ_CAPACITY_PER_SECOND = "dynacast.readCapacityPerSecond";
    
    String WRITE_CAPACITY_PER_SECOND = "dynacast.writeCapacityPerSecond"; 
    
    String CLUSTER_NAME = "dynacast.clusterName";
    
    String READ_AFTER_WRITE_SUPPORT = "dynacast.readAfterWriteSupport";
    
    String CLUSTER_HOSTING_ON_AWS = "dynacast.clusterHostingOnAWS";
    
    String CLUSTER_REGION_ON_AWS = "dynacast.clusterRegionOnAWS";
    
    String USE_PUBLIC_IPS_ON_AWS = "dynacast.usePublicIPsOnAWS";
    
    String CLIEN_MODE_ENABLED = "dynacast.clientModeEnabled";
    
    String LOCAL_CACHE_CAPACITY = "dynacast.localCacheCapacity";
    
    String DISTRIBUTED_CACHE_CAPACITY = "dynacast.distributedCacheCapacity";
    
}
