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

package io.openmessaging.connect.runtime.service;

import java.util.List;
import java.util.Map;

/**
 * Interface for position manager.
 */
public interface PositionManagementService {

    /**
     * Start the manager.
     */
    void start();

    /**
     * Stop the manager.
     */
    void stop();

    /**
     * Persist position info in a persist store.
     */
    void persist();

    /**
     * Get the current position table.
     * @return
     */
    Map<byte[], byte[]> getPositionTable();

    /**
     * Put a position info.
     */
    void putPosition(Map<byte[], byte[]> positions);

    /**
     * Remove a position info.
     * @param partitions
     */
    void removePosition(List<byte[]> partitions);

    /**
     * Register a listener.
     * @param listener
     */
    void registerListener(PositionManagementService.PositionUpdateListener listener);

    interface PositionUpdateListener {

        /**
         * Invoke while position info updated.
         */
        void onPositionUpdate();
    }
}
