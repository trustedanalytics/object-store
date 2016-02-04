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
import org.trustedanalytics.store.hdfs.fs.FsPermissionHelper;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

public class OrgSpecificHdfsObjectStore implements ObjectStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrgSpecificHdfsObjectStore.class);

    private final ImmutableList<String> technicalUsers;
    private final FileSystem hdfs;
    private final Path chrootPath;
    private final HdfsObjectStore hdfsObjectStore;

    public OrgSpecificHdfsObjectStore(ImmutableList<String> technicalUsers, FileSystem hdfs, String orgSpecificChrootUrl)
            throws IOException {

        this.technicalUsers = technicalUsers;
        this.hdfs = hdfs;
        this.chrootPath = new Path(orgSpecificChrootUrl);
        ensureDirExistsWithProperPermissions();
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
                FsPermissionHelper.getAclsForTechnicalUsers(technicalUsers, FsAction.READ_EXECUTE));

        List<AclEntry> actualAcls = hdfs.getAclStatus(path).getEntries();
        LOGGER.debug("ACLs for '" + objectId + "': " + aclsToString(actualAcls));
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

    private void ensureDirExistsWithProperPermissions() throws IOException {
        if (hdfs.exists(chrootPath)) {
            LOGGER.info("dir '{}' already exists", chrootPath);
        } else {
            LOGGER.info("dir '{}' does not exist, try to make it", chrootPath);
            hdfs.mkdirs(chrootPath);
            ensurePermissionsAreCorrect();
            ensureAclsAreCorrect();
        }
    }

    private void ensurePermissionsAreCorrect() throws IOException {
        FsPermission requiredPermission = FsPermissionHelper.permission770;
        LOGGER.info("try to set permission to " + requiredPermission);
        hdfs.setPermission(chrootPath, requiredPermission);

        //checking permission because 'setPermission' method does not guarantee to throw on error
        FsPermission actualPermission = hdfs.getFileStatus(chrootPath).getPermission();
        LOGGER.info("actual permission after set " + actualPermission);
        if (!requiredPermission.equals(actualPermission)) {
            throw new IOException("Cannot change permissions for " + chrootPath);
        }
    }

    private void ensureAclsAreCorrect() throws IOException {
        List<AclEntry> requiredAcls =
                FsPermissionHelper.getAclsForTechnicalUsers(technicalUsers, FsAction.EXECUTE);

        LOGGER.info("try to set acls to " + aclsToString(requiredAcls));
        hdfs.modifyAclEntries(chrootPath, requiredAcls);

        List<AclEntry> actualAcls = hdfs.getAclStatus(chrootPath).getEntries();
        LOGGER.info("actual Acls after set " + aclsToString(actualAcls));
    }

    private String aclsToString(List<AclEntry> requiredAcls) {
        return requiredAcls.stream().map(AclEntry::toString).collect(Collectors.joining(", "));
    }
}
