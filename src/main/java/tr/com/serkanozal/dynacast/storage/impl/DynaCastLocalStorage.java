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

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import tr.com.serkanozal.dynacast.storage.DynaCastStorage;
import tr.com.serkanozal.dynacast.storage.DynaCastStorageType;

class DynaCastLocalStorage<K, V> implements DynaCastStorage<K, V> {

    private static final Logger LOGGER = Logger.getLogger(DynaCastLocalStorage.class);
    
    private final String name;
    // No need to define map as volatile. 
    // Because in case of destroy, this will be seen as null by all threads eventually 
    // and so will be collected by GC.
    private NonBlockingHashMap<K, V> map = 
            new NonBlockingHashMap<K, V>();
    
    DynaCastLocalStorage(String name) {
        this.name = name;
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public DynaCastStorageType getType() {
        throw new UnsupportedOperationException();
    }
    
    private ConcurrentMap<K, V> provideMap() {
        ConcurrentMap<K, V> m = map;
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

    @Override
    public void put(K key, V value) {
        if (value == null) {
            remove(key);
        } else {
            provideMap().put(key, value);
            
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
