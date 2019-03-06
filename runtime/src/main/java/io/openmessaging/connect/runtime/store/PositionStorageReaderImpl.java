/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.openmessaging.connect.runtime.store;

import io.openmessaging.connect.runtime.service.PositionManagementService;
import io.openmessaging.connector.api.PositionStorageReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PositionStorageReaderImpl implements PositionStorageReader {

    private PositionManagementService positionManagementService;

    public PositionStorageReaderImpl(PositionManagementService positionManagementService){
        this.positionManagementService = positionManagementService;
    }

    @Override
    public byte[] getPosition(byte[] partition) {
        Map<byte[], byte[]> allData = positionManagementService.getPositionTable();

        for(Map.Entry<byte[], byte[]> entry : allData.entrySet()){
            if(Arrays.equals(entry.getKey(), partition)){
                return entry.getValue();
            }
        }
        return null;
    }

    @Override
    public Map<byte[], byte[]> getPositions(Collection<byte[]> partitions) {
        Map<byte[], byte[]> result = new HashMap<>();
        Map<byte[], byte[]> allData = positionManagementService.getPositionTable();
        for(Map.Entry<byte[], byte[]> entry : allData.entrySet()){
            for(byte[] partition : partitions){
                if(Arrays.equals(entry.getKey(), partition)){
                    result.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return result;
    }
}
