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

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import sun.misc.Unsafe;
import tr.com.serkanozal.dynacast.DynaCastConfigs;
import tr.com.serkanozal.dynacast.storage.DynaCastStorage;
import tr.com.serkanozal.dynacast.storage.DynaCastStorageType;

class DynaCastLocalStorage<K, V> implements DynaCastStorage<K, V> {

    private static final Logger LOGGER = Logger.getLogger(DynaCastLocalStorage.class);
    
    private static final Unsafe UNSAFE;
    private static final long KVS_OFFSET;
    private static final long OBJ_ARRAY_BASE_OFFSET;
    private static final int OBJ_ARRAY_INDEX_SCALE;
    private static final int MAX_EVICT_COUNT = 16;
    
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to access to 'sun.misc.Unsafe'", e);
        }
        
        OBJ_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(Object[].class);
        OBJ_ARRAY_INDEX_SCALE = UNSAFE.arrayIndexScale(Object[].class);
        
        try { 
            Field f = NonBlockingHashMap.class.getDeclaredField("_kvs"); 
            KVS_OFFSET = UNSAFE.objectFieldOffset(f);
        } catch (NoSuchFieldException e) { 
            throw new IllegalStateException(
                    "Unable to access to internal key-value array of 'NonBlockingHashMap'", e); 
        }
    }    
    
    private final String name;
    private final int capacity;
    private final ThreadLocal<Random> threadLocalRandom = 
            new ThreadLocal<Random>() {
                protected Random initialValue() {
                    return new Random();
                };
            };
    // No need to define map as volatile. 
    // Because in case of destroy, this will be seen as null by all threads eventually 
    // and so will be collected by GC.
    private NonBlockingHashMap<K, V> map = 
            new NonBlockingHashMap<K, V>();
    
    DynaCastLocalStorage(String name) {
        this(name, null);
    }
    
    DynaCastLocalStorage(String name, Map<String, Object> properties) {
        this.name = name;
        if (properties != null) {
            if (properties.get(DynaCastConfigs.LOCAL_CACHE_CAPACITY) != null) {
                capacity = Integer.parseInt(properties.get(DynaCastConfigs.LOCAL_CACHE_CAPACITY).toString());
            } else {
                capacity = DynaCastStorageManager.DEFAULT_LOCAL_CACHE_CAPACITY;
            }
        } else {
            capacity = DynaCastStorageManager.DEFAULT_LOCAL_CACHE_CAPACITY;
        }
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public DynaCastStorageType getType() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isAvailable() {
        return map != null;
    }
    
    private NonBlockingHashMap<K, V> provideMap() {
        NonBlockingHashMap<K, V> m = map;
        if (m == null) {
            throw new IllegalStateException(
                    String.format("Local storage '%s' has been already destroyed!", name));
        }
        return m;
    }

    @Override
    public V get(K key) {
        V value = provideMap().get(key);
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    String.format("Value %s has been retrieved from the local storage '%s' with key %s", 
                                  value, name, key));
        }
        
        return value;
    }
    
    @Override
    public V refresh(K key) {
        return get(key);
    }

    @SuppressWarnings("unchecked")
    private void evict(NonBlockingHashMap<K, V> m) {
        int evictCount = 0;
        Object[] kvs = (Object[]) UNSAFE.getObjectVolatile(m, KVS_OFFSET);
        int capacity = (kvs.length - 2) >> 1; 
        int capacityMask = capacity - 1;
        Random random = threadLocalRandom.get();
        int randomIdx = random.nextInt(capacity); // capacity is always power of 2
        
        for (int i = 0, idx = randomIdx; i < MAX_EVICT_COUNT || evictCount == 0; i++) {
            int keyIdx = 2 + (idx << 1);
            K key = (K) UNSAFE.getObjectVolatile(kvs, OBJ_ARRAY_BASE_OFFSET + OBJ_ARRAY_INDEX_SCALE * keyIdx);
            if (key != null && m.remove(key) != null) {
                idx = random.nextInt(capacity);
                evictCount++;
            } else {
                idx = (idx + 1) & capacityMask;
            }
            if (i > capacity) {
                break;
            }
        }
    }

    @Override
    public void put(K key, V value) {
        if (value == null) {
            remove(key);
        } else {
            NonBlockingHashMap<K, V> m = provideMap();
            // Note that there is no %100 guarantee for being under capacity always.
            // Size may overflow capacity when there are simultaneous accesses here (capacity check + put)
            if (capacity > 0 && m.size() >= capacity) {
                evict(m);
            }
            m.put(key, value);
            
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        String.format("Value %s has been put into the local storage '%s' with key %s", 
                                      value, name, key));
            }
        }    
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        boolean replaced = false;
        if (oldValue == null && newValue != null) {
            if (provideMap().putIfAbsent(key, newValue) == null) {
                replaced = true;
            }
        } else if (oldValue != null && newValue == null) {
            replaced = provideMap().remove(key, oldValue);
        } else if (oldValue != null && newValue != null) {
            replaced = map.replace(key, oldValue, newValue);
        }    
        
        if (replaced && LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    String.format("Old value %s has been replaced with new value %s " + 
                                  "assigned to key %s in the local storage '%s'", 
                                  oldValue, newValue, key, name));
        }
        
        return replaced;
    }

    @Override
    public void remove(K key) {
        provideMap().remove(key);
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    String.format("Value has been removed from the local storage '%s' with key %s", 
                                  name, key));
        }
    }
    
    @Override
    public void clear() {
        Iterator<K> iter = provideMap().keySet().iterator();
        while (iter.hasNext()) {
            K key = iter.next();
            remove(key);
        }
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("Local storage '%s' has been cleared", name));
        }
    }
    
    @Override
    public synchronized void destroy() {
        if (map == null) {
            return;
        }
        
        clear();
        map = null;
        
        LOGGER.info(String.format("Local storage '%s' has been destroyed", name));
    }

    
}
