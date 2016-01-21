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

import org.trustedanalytics.store.ObjectStore;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class OrgSpecificHdfsObjectStore implements ObjectStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrgSpecificHdfsObjectStore.class);

    private final String cfUser;
    private final String hiveUser;
    private final FileSystem hdfs;
    private final Path chrootPath;
    private final HdfsObjectStore hdfsObjectStore;

    public OrgSpecificHdfsObjectStore(String cfUser, String hiveUser, FileSystem hdfs, String orgSpecificChrootUrl)
        throws IOException {

        this.cfUser = cfUser;
        this.hiveUser = hiveUser;
        this.hdfs = hdfs;
        this.chrootPath = new Path(orgSpecificChrootUrl);
        new Prerequisites(hdfs).prepareDir(chrootPath);
        this.hdfsObjectStore = new HdfsObjectStore(hdfs, chrootPath);
    }

    @Override
    public String save(InputStream input) throws IOException {
        ObjectId objectId = hdfsObjectStore.saveObject(input);
        setAClsForTechnicalUsers(objectId);
        return objectId.toString();
    }

    private void setAClsForTechnicalUsers(ObjectId objectId) throws IOException {
        LOGGER.debug("setAClsForTechnicalUsers objectId = [" + objectId + "]");
        Path path = getPath(chrootPath, objectId.getDirectoryName().toString());
        hdfs.modifyAclEntries(path,
            FsPermissionHelper.getAclsForTechnicalUsers(Arrays.asList(cfUser, hiveUser), FsAction.READ_EXECUTE));
        hdfs.getAclStatus(path).getEntries().stream().map(AclEntry::toString).forEach(LOGGER::debug);
    }

    private Path getPath(Path basicPath, String objectDirName) {
        return new Path(basicPath + "/" + objectDirName);
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
            LOGGER.info("prepare dir " + path);
            if (hdfs.exists(path)) {
                LOGGER.info("dir exists");
            } else {
                FsPermission requiredPermission = FsPermissionHelper.getPermission770();

                LOGGER.info("dir not exist, try to make it with permission " + requiredPermission);
                hdfs.mkdirs(path, requiredPermission);
                FsPermission actualPermission = hdfs.getFileStatus(path).getPermission();
                LOGGER.info("actual permission " + actualPermission);

                LOGGER.info("try to change permission to " + requiredPermission);
                hdfs.setPermission(path, requiredPermission);
                actualPermission = hdfs.getFileStatus(path).getPermission();
                LOGGER.info("actual permission after change " + actualPermission);

                hdfs.modifyAclEntries(path,
                    FsPermissionHelper.getAclsForTechnicalUsers(Arrays.asList(cfUser, hiveUser), FsAction.EXECUTE));
            }
        }
    }
}
