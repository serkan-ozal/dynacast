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

import java.util.Map;

import org.apache.log4j.Logger;

import tr.com.serkanozal.dynacast.storage.DynaCastStorage;
import tr.com.serkanozal.dynacast.storage.DynaCastStorageType;
import tr.com.serkanozal.dynacast.storage.impl.DynaCastStorageManager;

public class DynaCast {

    public static final String VERSION = "1.0-RELEASE";
    
    private static final Logger LOGGER = Logger.getLogger(DynaCast.class);
    
    public static <K, V> DynaCastStorage<K, V> getStorage(String storageName) {
        return DynaCastStorageManager.getStorage(storageName);
    }
    
    public static <K, V> DynaCastStorage<K, V> getOrCreateStorage(
            String storageName, 
            DynaCastStorageType storageType) {
        return DynaCastStorageManager.getOrCreateStorage(storageName, storageType);
    }
    
    public static <K, V> DynaCastStorage<K, V> getOrCreateStorage(
            String storageName, 
            DynaCastStorageType storageType,
            Map<String, Object> properties) {
        return DynaCastStorageManager.getOrCreateStorage(storageName, storageType, properties);
    }
    
    public static <K, V> DynaCastStorage<K, V> deleteStorage(String storageName, boolean destroy) {
        return DynaCastStorageManager.deleteStorage(storageName, destroy);
    }
	
    @SuppressWarnings("rawtypes")
    public static void main(String[] args) {
        System.out.println();
        System.out.println(" .----------------.  .----------------.  .-----------------. .----------------.  .----------------.  .----------------.  .----------------.  .----------------."); 
        System.out.println("| .--------------. || .--------------. || .--------------. || .--------------. || .--------------. || .--------------. || .--------------. || .--------------. |"); 
        System.out.println("| |  ________    | || |  ____  ____  | || | ____  _____  | || |      __      | || |     ______   | || |      __      | || |    _______   | || |  _________   | |"); 
        System.out.println("| | |_   ___ `.  | || | |_  _||_  _| | || ||_   \\|_   _| | || |     /  \\     | || |   .' ___  |  | || |     /  \\     | || |   /  ___  |  | || | |  _   _  |  | |"); 
        System.out.println("| |   | |   `. \\ | || |   \\ \\  / /   | || |  |   \\ | |   | || |    / /\\ \\    | || |  / .'   \\_|  | || |    / /\\ \\    | || |  |  (__ \\_|  | || | |_/ | | \\_|  | |"); 
        System.out.println("| |   | |    | | | || |    \\ \\/ /    | || |  | |\\ \\| |   | || |   / ____ \\   | || |  | |         | || |   / ____ \\   | || |   '.___`-.   | || |     | |      | |"); 
        System.out.println("| |  _| |___.' / | || |    _|  |_    | || | _| |_\\   |_  | || | _/ /    \\ \\_ | || |  \\ `.___.'\\  | || | _/ /    \\ \\_ | || |  |`\\____) |  | || |    _| |_     | |"); 
        System.out.println("| | |________.'  | || |   |______|   | || ||_____|\\____| | || ||____|  |____|| || |   `._____.'  | || ||____|  |____|| || |  |_______.'  | || |   |_____|    | |"); 
        System.out.println("| |              | || |              | || |              | || |              | || |              | || |              | || |              | || |              | |"); 
        System.out.println("| '--------------' || '--------------' || '--------------' || '--------------' || '--------------' || '--------------' || '--------------' || '--------------' |"); 
        System.out.println(" '----------------'  '----------------'  '----------------'  '----------------'  '----------------'  '----------------'  '----------------'  '----------------'");  
        System.out.println();
        
        LOGGER.info(
                "\nStarting DynaCast " + "(" + "version=" + VERSION + ")" + " in " + 
                (DynaCastStorageManager.isClienModeEnabled() ? "client" : "server") + 
                " mode ...\n");
        Map<String, String> props = DynaCastStorageManager.getProperties();
        if (!props.isEmpty()) {
            LOGGER.info("Properties :");
            for (Map.Entry<String, String> entry : props.entrySet()) {
                LOGGER.info("\t- " + entry.getKey() + "=" + entry.getValue());
            }
        }    
        Map<String, DynaCastStorage> storages = DynaCastStorageManager.getStorages();
        if (!storages.isEmpty()) {
            LOGGER.info("Storages   :");
            for (DynaCastStorage storage : storages.values()) {
                LOGGER.info("\t- " + "name=" + storage.getName() + ", type=" + storage.getType());
            }
        }    
    }
    
}
