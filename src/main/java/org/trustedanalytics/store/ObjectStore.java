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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

// Simplified for now - skipping URIs

public interface ObjectStore {

    /**
     * @return unique id which allows to find given Object
     */
    String save(InputStream input, String dataSetName) throws IOException;

    default String save(byte[] bytes, String dataSetName) throws IOException {
        return save(new ByteArrayInputStream(bytes), dataSetName);
    }

    InputStream getContent(String objectId) throws IOException;

    default void remove(String objectId) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * @return id of the store so that it can be later on accessed
     */
    String getId();
}
