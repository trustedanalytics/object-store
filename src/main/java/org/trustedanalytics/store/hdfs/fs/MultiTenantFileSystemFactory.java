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

import org.trustedanalytics.hadoop.config.client.Property;
import org.trustedanalytics.hadoop.config.client.ServiceInstanceConfiguration;
import org.trustedanalytics.hadoop.config.client.oauth.TapOauthToken;
import org.trustedanalytics.kerberos.OAuthKerberosClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import javax.security.auth.login.LoginException;
import java.io.IOException;

public class MultiTenantFileSystemFactory implements OAuthSecuredFileSystemFactory {

    private final ServiceInstanceConfiguration hdfsConf;
    private final ServiceInstanceConfiguration krbConf;
    private final OAuthKerberosClient oAuthKerberosClient;
    private final ApacheFileSystemFactory apacheFileSystemFactory;

    public MultiTenantFileSystemFactory(ServiceInstanceConfiguration hdfsConf, ServiceInstanceConfiguration krbConf,
            OAuthKerberosClient oAuthKerberosClient, ApacheFileSystemFactory apacheFileSystemFactory) {
        this.hdfsConf = hdfsConf;
        this.krbConf = krbConf;
        this.oAuthKerberosClient = oAuthKerberosClient;
        this.apacheFileSystemFactory = apacheFileSystemFactory;
    }

    @Override
    public FileSystem getFileSystem(String oAuthToken)
            throws IOException, InterruptedException, LoginException {

        //TODO: this code is from hadoop-utils (95%),
        // we need to change hadoop-utils to enable using uri with templates like hdfs://name/org/%{organization}/catalog
        // after that, delete also OAuthKerberosClient and TapOAuthKerberosClient
        TapOauthToken jwtToken = new TapOauthToken(oAuthToken);
        Configuration hadoopConf = hdfsConf.asHadoopConfiguration();
        oAuthKerberosClient.loginIfKerberosEnabled(hadoopConf, krbConf, jwtToken);
        return apacheFileSystemFactory.get(hadoopConf, jwtToken);
    }

    @Override
    public String getHdfsUri(String org) {
      return MultiTenantPathTemplate.resolveOrg(hdfsConf.getProperty(Property.HDFS_URI).get(), org);
    }
}
