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
package org.trustedanalytics.store.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OrgSpecificHdfsObjectStoreTest {

    private static final String URL = "hdfs://nameservice1/some_dir/";

    @Test
    public void createOrgSpecificHdfsObjectStore_fileSystemWorks_directoryCreated()
        throws IOException {

        FileSystem fs = mock(FileSystem.class);
        new OrgSpecificHdfsObjectStore(fs, URL);
        verify(fs).mkdirs(new Path(URL));
    }

    @Test(expected = IOException.class)
    public void createOrgSpecificHdfsObjectStore_fileSystemThrowsException_exceptionRethrown()
        throws IOException {

        FileSystem fs = mock(FileSystem.class);
        String url = "hdfs://nameservice1/some_dir/";
        when(fs.mkdirs(new Path(URL))).thenThrow(IOException.class);
        new OrgSpecificHdfsObjectStore(fs, url);
    }
}
