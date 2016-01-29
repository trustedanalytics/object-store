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
package org.trustedanalytics.store.hdfs.fs;

import org.trustedanalytics.hadoop.config.client.AppConfiguration;
import org.trustedanalytics.hadoop.config.client.Property;
import org.trustedanalytics.hadoop.config.client.ServiceType;
import org.trustedanalytics.hadoop.config.client.helper.Hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URISyntaxException;

public class SingleTenantFileSystemFactory implements FileSystemFactory {

    private final AppConfiguration appConfiguration;

    public SingleTenantFileSystemFactory(AppConfiguration appConfiguration) {
        this.appConfiguration = appConfiguration;
    }

    public FileSystem getFileSystem() throws IOException, LoginException, InterruptedException, URISyntaxException {
        return Hdfs.newInstance().createFileSystem();
    }

    public Path getChrootedPath() throws IOException {
        String hdfsUri = getHdfsUriFromConfiguration(appConfiguration);
        return new Path(hdfsUri);
    }

    private String getHdfsUriFromConfiguration(AppConfiguration helper) {
        return helper.getServiceConfig(ServiceType.HDFS_TYPE).getProperty(Property.HDFS_URI).get();
    }
}
