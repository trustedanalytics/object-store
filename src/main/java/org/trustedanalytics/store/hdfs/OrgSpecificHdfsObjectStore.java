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
import org.trustedanalytics.store.ObjectStore;

import java.io.IOException;
import java.io.InputStream;

public class OrgSpecificHdfsObjectStore implements ObjectStore {

    private final HdfsObjectStore hdfsObjectStore;

    public OrgSpecificHdfsObjectStore(FileSystem hdfs, String orgSpecificChrootUrl)
        throws IOException {

        Path chrootPath = new Path(orgSpecificChrootUrl);
        new Prerequisites(hdfs).prepareDir(chrootPath);
        this.hdfsObjectStore = new HdfsObjectStore(hdfs, chrootPath);
    }

    @Override
    public String save(InputStream input) throws IOException {
        return hdfsObjectStore.save(input);
    }

    @Override
    public InputStream getContent(String objectId) throws IOException {
        return hdfsObjectStore.getContent(objectId);
    }

    @Override
    public void remove(String objectId) throws IOException {
        hdfsObjectStore.remove(objectId);
    }

    @Override
    public String getId() {
        return hdfsObjectStore.getId();
    }

    private class Prerequisites {

        private final FileSystem hdfs;

        private Prerequisites(FileSystem hdfs) {
            this.hdfs = hdfs;
        }

        private void prepareDir(Path path) throws IOException {
            hdfs.mkdirs(path);
        }
    }
}
