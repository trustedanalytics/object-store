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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trustedanalytics.hadoop.config.ConfigurationHelper;
import org.trustedanalytics.hadoop.config.ConfigurationLocator;
import org.trustedanalytics.hadoop.config.PropertyLocator;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManager;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManagerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import javax.security.auth.login.LoginException;

public class FileSystemFactoryImpl implements FileSystemFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemFactoryImpl.class);

  private static final String AUTHENTICATION_METHOD = "kerberos";

  private static final String AUTHENTICATION_METHOD_PROPERTY = "hadoop.security.authentication";

  private ConfigurationHelper confHelper;

  private KrbLoginManagerFactory loginManagerFactory;

  private Configuration hadoopConf = new Configuration(false);

  public FileSystemFactoryImpl(ConfigurationHelper confHelper,
                        KrbLoginManagerFactory loginManagerFactory) {
    this.confHelper = confHelper;
    this.loginManagerFactory = loginManagerFactory;
  }

  public FileSystem getFileSystem() throws IOException, LoginException, InterruptedException {

    Map<String, String> params = confHelper.getConfigurationFromEnv(ConfigurationLocator.HADOOP);
    params.forEach(hadoopConf::set);

    if(AUTHENTICATION_METHOD.equals(hadoopConf.get(AUTHENTICATION_METHOD_PROPERTY))) {
      return getSecureFileSystem();
    } else {
      return getInsecureFileSystem();
    }
  }

  public Path getChrootedPath() throws IOException {
    return new Path(getPropertyFromCredentials(PropertyLocator.HDFS_URI));
  }

  public FileSystem getSecureFileSystem() throws IOException, LoginException,
                                                  InterruptedException {
    LOGGER.info("Trying kerberos auth");
    KrbLoginManager loginManager = loginManagerFactory.getKrbLoginManagerInstance(
            getPropertyFromCredentials(PropertyLocator.KRB_KDC),
            getPropertyFromCredentials(PropertyLocator.KRB_REALM));

    loginManager.loginInHadoop(loginManager.loginWithCredentials(
        getPropertyFromCredentials(PropertyLocator.USER),
        getPropertyFromCredentials(PropertyLocator.PASSWORD).toCharArray()), hadoopConf);

    LOGGER.info("Creating filesytem with kerberos auth");
    return FileSystem.get(prepareHdfsUri(),
                          hadoopConf, getPropertyFromCredentials(PropertyLocator.USER));
  }

  public FileSystem getInsecureFileSystem() throws IOException, InterruptedException {
    LOGGER.info("Creating filesytem without kerberos auth");
    return FileSystem.get(prepareHdfsUri(),
                          hadoopConf, getPropertyFromCredentials(PropertyLocator.USER));
  }

  private String getPropertyFromCredentials(PropertyLocator property) throws IOException {
    return confHelper.getPropertyFromEnv(property)
        .orElseThrow(() -> new IllegalStateException(
            property.name() + " not found in VCAP_SERVICES"));
  }

  private URI prepareHdfsUri() {
    try {
      return new URI(getPropertyFromCredentials(PropertyLocator.HDFS_URI));
    } catch (URISyntaxException | IOException e) {
      throw new IllegalArgumentException("Invalid hdfs uri!", e);
    }
  }
}
