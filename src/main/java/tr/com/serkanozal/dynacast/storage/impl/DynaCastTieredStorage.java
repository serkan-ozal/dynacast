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

import java.util.Map;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.log4j.Logger;

import tr.com.serkanozal.dynacast.storage.DynaCastStorage;
import tr.com.serkanozal.dynacast.storage.DynaCastStorageType;
import tr.com.serkanozal.dynacast.storage.impl.DynaCastDistributedStorage.StorageMutationListener;

public class DynaCastTieredStorage<K, V> implements DynaCastStorage<K, V> {

    private static final Logger LOGGER = Logger.getLogger(DynaCastTieredStorage.class);
    
    private final String name;
    private final NearCache nearCache;
    private final DynaCastDistributedStorage<K, V> distributedStorage;
    
    public DynaCastTieredStorage(String name, Map<String, Object> properties) {
        this.name = name;
        this.nearCache = 
                new NearCache(new DynaCastLocalStorage<K, V>(name));
        this.distributedStorage = 
                new DynaCastDistributedStorage<K, V>(
                        name, 
                        properties,
                        new TieredStorageAwareStorageChangeListener());
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    private class TieredStorageAwareStorageChangeListener implements StorageMutationListener<K, V> {
        
        private void invalidate(K key) {
            long ownId = nearCache.tryOwn(key);
            try {
                nearCache.remove(key);
            } finally {
                nearCache.releaseIfOwned(ownId, key);
            }
            
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        String.format("Entry has been invalidated from " + 
                                      "near-cache of tiered storage with key %s", key));
            }
        }
        
        @Override
        public void onInsert(K key, V value) {
            invalidate(key);
        }
        
        @Override
        public void onUpdate(K key, V oldValue, V newValue) {
            invalidate(key);
        }

        @Override
        public void onDelete(K key) {
            invalidate(key);
        }
        
    }
    
    @Override
    public DynaCastStorageType getType() {
        return DynaCastStorageType.TIERED;
    }

    @Override
    public V get(K key) {
        V value = nearCache.get(key);
        if (value != null) {
            return value;
        }
        
        long ownId = nearCache.tryOwn(key);
        try {
            value = distributedStorage.get(key);
            if (value != null) {
                nearCache.putIfAvailable(ownId, key, value);
            }    
        } finally {
            nearCache.releaseIfOwned(ownId, key);
        }
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    String.format("Value %s has been retrieved from tiered storage with key %s", key, value));
        }
        
        return value;
    }
    
    @Override
    public V refresh(K key) {
        V value = null;
        long ownId = nearCache.tryOwn(key);
        try {
            nearCache.remove(key);
            value = distributedStorage.get(key);
            if (value != null) {
                nearCache.putIfAvailable(ownId, key, value);
            }    
        } finally {
            nearCache.releaseIfOwned(ownId, key);
        }
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    String.format("Value %s has been refreshed from tiered storage with key %s", key, value));
        }
        
        return value;
    }

    @Override
    public void put(K key, V value) {
        if (value == null) {
            remove(key);
        } else {
            long ownId = nearCache.tryOwn(key);
            try {
                distributedStorage.put(key, value);
                nearCache.putIfAvailable(ownId, key, value);  
            } finally {
                nearCache.releaseIfOwned(ownId, key);
            }
            
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        String.format("Value %s has been put into tiered storage with key %s", key, value));
            }
        }
    }
    
    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        boolean replaced = false;
        if (oldValue == null && newValue != null) {
            long ownId = nearCache.tryOwn(key);
            try {
                if (distributedStorage.replace(key, oldValue, newValue)) {
                    nearCache.putIfAvailable(ownId, key, newValue); 
                    replaced = true;
                }
            } finally {
                nearCache.releaseIfOwned(ownId, key);
            }
        } else if (oldValue != null && newValue == null) {
            long ownId = nearCache.tryOwn(key);
            try {
                if (distributedStorage.replace(key, oldValue, newValue)) {
                    nearCache.remove(key);
                    replaced = true;
                }
            } finally {
                nearCache.releaseIfOwned(ownId, key);
            }
        } else if (oldValue != null && newValue != null) {
            long ownId = nearCache.tryOwn(key);
            try {
                if (distributedStorage.replace(key, oldValue, newValue)) {
                    nearCache.putIfAvailable(ownId, key, newValue); 
                    replaced = true;
                }
            } finally {
                nearCache.releaseIfOwned(ownId, key);
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
        long ownId = nearCache.tryOwn(key);
        try {
            distributedStorage.remove(key);
            nearCache.remove(key);
        } finally {
            nearCache.releaseIfOwned(ownId, key);
        }
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    String.format("Value has been removed from tiered storage with key %s", key));
        }
    }
    
    @Override
    public void clear() {
        nearCache.ownAll();
        try {
            distributedStorage.clear();
            nearCache.clear();
        } finally {
            nearCache.releaseAll();
        }
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Tiered storage has been cleared");
        }
    }
    
    @Override
    public void destroy() {
        distributedStorage.destroy();
        nearCache.destroy();
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Tiered storage has been destroyed");
        }
    }
    
    private class NearCache {

        private final int SLOT_COUNT = 1024;
        private final int SLOT_MASK = SLOT_COUNT - 1;
        
        private final AtomicLongArray slotStates = new AtomicLongArray(SLOT_COUNT * 4);
        private final DynaCastStorage<K, V> localStorage;
        
        private NearCache(DynaCastStorage<K, V> localStorage) {
            this.localStorage = localStorage;
        }
        
        private int getSlot(K key) {
            int hash = key.hashCode();
            return hash & SLOT_MASK;
        }
        
        private int ownIdIndex(int slot) {
            return (slot << 2);
        }
        
        private int activeCountIndex(int slot) {
            return (slot << 2) + 1;
        }
        
        private int completedCountIndex(int slot) {
            return (slot << 2) + 2;
        }

        private long tryOwn(K key) {
            long ownId = -1;
            int slot = getSlot(key);
            long currentCompleted = slotStates.get(completedCountIndex(slot));
            if (slotStates.compareAndSet(ownIdIndex(slot), 0, currentCompleted)) {
                ownId = currentCompleted;
            }
            slotStates.incrementAndGet(activeCountIndex(slot));
            return ownId;
        }
        
        private void ownAll() {
            for (int slot = 0; slot < SLOT_COUNT; slot++) {
                slotStates.incrementAndGet(activeCountIndex(slot));
            }    
        }

        private void releaseIfOwned(long ownId, K key) {
            int slot = getSlot(key);
            slotStates.incrementAndGet(completedCountIndex(slot));
            slotStates.decrementAndGet(activeCountIndex(slot));
            if (ownId >= 0) {
                slotStates.set(ownIdIndex(slot), 0);
            }   
        }
        
        private void releaseAll() {
            for (int slot = 0; slot < SLOT_COUNT; slot++) {
                slotStates.incrementAndGet(completedCountIndex(slot));
                slotStates.decrementAndGet(activeCountIndex(slot));
            }
        }
        
        private boolean putIfAvailable(long ownId, K key, V value) {
            if (ownId >= 0) {
                int slot = getSlot(key);
                long activeCount = slotStates.get(activeCountIndex(slot));
                long expectedCompleted = ownId;
                long currentCompleted = slotStates.get(completedCountIndex(slot));
                if (activeCount == 1 && currentCompleted == expectedCompleted) {
                    put(key, value);
                    return true;
                }   
            }
            return false;
        }

        private V get(K key) {
            return localStorage.get(key);
        }

        private void put(K key, V value) {
            localStorage.put(key, value);
        }

        private void remove(K key) {
            localStorage.remove(key);
        }

        private void clear() {
            localStorage.clear();
        }
        
        private void destroy() {
            localStorage.destroy();
        }

    }   

}
