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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.trustedanalytics.store.MemoryObjectStore;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.ByteStreams;


public class MemoryObjectStoreTest {

    private MemoryObjectStore store;

    @Before
    public void initialize() {
        store = new MemoryObjectStore();
    }

    @Test
    public void testBasicCase() throws Exception {
        byte[] bytes1 = new byte[]{1, 2, 3, 4};
        String id1 = store.save(bytes1);

        byte[] bytes2 = new byte[]{1, 2};
        String id2 = store.save(bytes2);

        assertNotEquals(id1, id2);

        InputStream in1 = store.getContent(id1);
        InputStream in2 = store.getContent(id2);
        assertTrue(Arrays.equals(bytes1, ByteStreams.toByteArray(in1)));
        assertTrue(Arrays.equals(bytes2, ByteStreams.toByteArray(in2)));
    }

    @Test(expected = FileNotFoundException.class)
    public void testGetContentWhenDoesntExist() throws Exception {
        store.getContent("this key doesn't exist in store");
    }

    @Test(expected = NoSuchElementException.class)
    public void remove_nonExistingObject_throwsNoSuchElementExceptionExpected() throws IOException {
        store.remove("nonExistingObjectId");
    }

    @Test
    public void remove_removingExistingObject_removed() throws IOException {
        String id = addDataToStore();

        assertNotNull(store.getContent(id));
        store.remove(id);
        assertRemovedFromStore(id);

    }

    private String addDataToStore(){
        byte[] bytes1 = new byte[]{1, 2, 3, 4};
        return store.save(bytes1);
    }

    private void assertRemovedFromStore(String id) throws IOException {
        try {
            store.getContent(id);
            fail("ObjectId still exist in data store");
        } catch (FileNotFoundException ex){

        }
    }

}
