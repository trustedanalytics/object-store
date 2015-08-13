/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.clients.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Storage with map-like interface. Here keys are generated uniquely.
 *
 * It is thread safe
 */
public class MemoryUniqGeneratedKeysStore<V> {

    private AtomicInteger count = new AtomicInteger(0);
    private ConcurrentMap<String, V> data = new ConcurrentHashMap<>();

    public V get(String id) {
        return data.get(id);
    }

    /**
     * Returns id generated for new value
     */
    public String put(V value) {
        String id = "" + count.incrementAndGet();
        data.put(id, value);
        return id;
    }

    public V remove(String id) {
        return data.remove(id);
    }

    public boolean has(String value) {
        return data.containsValue(value);
    }

    public boolean containsKey(String id) {
        return data.containsKey(id);
    }
}
