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

class DynaCastTieredStorage<K, V> implements DynaCastStorage<K, V> {

    private static final Logger LOGGER = Logger.getLogger(DynaCastTieredStorage.class);
    
    private final String name;
    private final NearCache nearCache;
    private final DynaCastDistributedStorage<K, V> distributedStorage;
    private volatile boolean destroyed = false;
    
    DynaCastTieredStorage(String name, DynaCastStorageType storageType, Map<String, Object> properties) {
        this.name = name;
        this.nearCache = 
                new NearCache(
                        new DynaCastLocalStorage<K, V>(name, properties));
        this.distributedStorage = 
                new DynaCastDistributedStorage<K, V>(
                        name, 
                        storageType,
                        properties,
                        new TieredStorageAwareStorageChangeListener());
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
                                      "near-cache of the tiered storage '%s' with key %s", 
                                      name, key));
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
    
    private void ensureAvailable() {
        if (destroyed) {
            throw new IllegalStateException(
                    String.format("Tiered storage '%s' has been destroyed!", name));
        }
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public DynaCastStorageType getType() {
        return DynaCastStorageType.TIERED;
    }
    
    @Override
    public boolean isAvailable() {
        return !destroyed;
    }

    @Override
    public V get(K key) {
        ensureAvailable();
        
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
                    String.format("Value %s has been retrieved from the tiered storage '%s' with key %s", 
                                  value, name, key));
        }
        
        return value;
    }
    
    @Override
    public V refresh(K key) {
        ensureAvailable();
        
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
                    String.format("Value %s has been refreshed from the tiered storage '%s' with key %s", 
                                  value, name, key));
        }
        
        return value;
    }

    @Override
    public void put(K key, V value) {
        ensureAvailable();
        
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
                        String.format("Value %s has been put into the tiered storage '%s' with key %s", 
                                      value, name, key));
            }
        }
    }
    
    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        ensureAvailable();
        
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
                                  "assigned to key %s in the tiered storage '%s'", 
                                  oldValue, newValue, key, name));
        }
        
        return replaced;
    }

    @Override
    public void remove(K key) {
        ensureAvailable();
        
        long ownId = nearCache.tryOwn(key);
        try {
            distributedStorage.remove(key);
            nearCache.remove(key);
        } finally {
            nearCache.releaseIfOwned(ownId, key);
        }
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    String.format("Value has been removed from the tiered storage '%s' with key %s", 
                                  name, key));
        }
    }
    
    @Override
    public void clear() {
        ensureAvailable();
        
        nearCache.ownAll();
        try {
            distributedStorage.clear();
            nearCache.clear();
        } finally {
            nearCache.releaseAll();
        }
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("Tiered storage '%s' has been cleared", name));
        }
    }
    
    @Override
    public synchronized void destroy() {
        if (destroyed) {
            return;
        }
        
        try {
            distributedStorage.destroy();
            nearCache.destroy();
            
            LOGGER.info(String.format("Tiered storage '%s' has been destroyed", name));
        } finally {
            destroyed = true;
        }
    }
    
}
