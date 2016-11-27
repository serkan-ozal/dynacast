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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import tr.com.serkanozal.dynacast.storage.DynaCastStorage;
import tr.com.serkanozal.dynacast.storage.DynaCastStorageType;

public final class DynaCastStorageProvider {

    @SuppressWarnings("rawtypes")
    private static final ConcurrentMap<String, DynaCastStorage> STORAGE_MAP = 
            new ConcurrentHashMap<String, DynaCastStorage>();

    private DynaCastStorageProvider() {
        
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
                return new DynaCastDistributedStorage<K, V>(storageName, properties);
            case TIERED:
                return new DynaCastTieredStorage<K, V>(storageName, properties);
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

}
