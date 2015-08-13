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
package org.trustedanalytics.store;

import com.google.common.io.ByteStreams;
import org.trustedanalytics.clients.utils.MemoryUniqGeneratedKeysStore;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;

@Component
@Profile("default")
public class MemoryObjectStore implements ObjectStore {

    private MemoryUniqGeneratedKeysStore<byte[]> data = new MemoryUniqGeneratedKeysStore<>();

    @Override
    public String save(InputStream input) throws IOException {
        return save(ByteStreams.toByteArray(input));
    }

    @Override
    public String save(byte[] bytes) {
        return data.put(bytes);
    }

    @Override
    public InputStream getContent(String objectId) throws IOException {
        byte[] bytes = data.get(objectId);
        if (bytes == null)
            throw new FileNotFoundException("No object with id: " + objectId);
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public void remove(String objectId) throws IOException {
        if (!data.containsKey(objectId)) {
            throw new NoSuchElementException();
        }
        data.remove(objectId);
    }

    @Override
    public String getId() {
        return "in_memory";
    }
}
