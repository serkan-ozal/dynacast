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
package tr.com.serkanozal.dynacast.storage;

import junit.framework.AssertionFailedError;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tr.com.serkanozal.dynacast.storage.DynaCastStorage;
import tr.com.serkanozal.dynacast.storage.DynaCastStorageType;
import tr.com.serkanozal.dynacast.storage.impl.DynaCastStorageManager;

public abstract class BaseDynaCastStorageTest {

    protected DynaCastStorageType storageType;
    protected DynaCastStorage<String, String> storage1;
    protected DynaCastStorage<String, String> storage2;
    
    @Before
    public void setup() {
        storageType = getStorageType();
        storage1 = DynaCastStorageManager.createStorage("TestStorage", storageType);
        storage1.clear();
        storage2 = DynaCastStorageManager.createStorage("TestStorage", storageType);
        storage2.clear();
    }
    
    @After
    public void tearDown() {
        storage1.destroy();
        storage2.destroy();
    }
    
    protected abstract DynaCastStorageType getStorageType();
    
    @Test
    public void test_storage() {
        Assert.assertNull(storage1.get("key-1"));
        Assert.assertNull(storage2.get("key-1"));
        
        ////////////////////////////////////////////////////////// 
        
        storage1.put("key-1", "value-1");
        storage2.put("key-2", "value-2");
        
        verify(storage1, "key-2", "value-2");
        verify(storage2, "key-1", "value-1");
        
        //////////////////////////////////////////////////////////
        
        storage1.put("key-1", "value-3");
        storage2.put("key-2", "value-4");
        
        verify(storage1, "key-2", "value-4");
        verify(storage2, "key-1", "value-3");
        
        //////////////////////////////////////////////////////////
        
        storage1.remove("key-1");
        storage2.remove("key-2");
        
        verify(storage1, "key-2", null);
        verify(storage2, "key-1", null);
    }
    
    private void verify(DynaCastStorage<String, String> storage, String key, String expectedValue) {
        long start = System.currentTimeMillis();
        long finish = start + 30 * 1000; // 30 seconds later
        while (System.currentTimeMillis() < finish) {
            if (expectedValue == null) {
                if (storage.get(key) == null) {
                    return;
                }
            } else {
                if (expectedValue.equals(storage.get(key))) {
                    return;
                }
            }    
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        throw new AssertionFailedError(
                String.format("Expected value %s couldn't be retrieved eventually!", expectedValue));
        
    }
    
}
