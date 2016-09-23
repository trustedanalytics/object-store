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

import org.trustedanalytics.hadoop.config.client.AppConfiguration;
import org.trustedanalytics.hadoop.config.client.Configurations;
import org.trustedanalytics.hadoop.config.client.Property;
import org.trustedanalytics.hadoop.config.client.ServiceInstanceConfiguration;
import org.trustedanalytics.hadoop.config.client.ServiceType;
import org.trustedanalytics.kerberos.TapOAuthKerberosClient;
import org.trustedanalytics.store.config.HdfsProperties;
import org.trustedanalytics.store.config.SimpleInstanceConfiguration;
import org.trustedanalytics.store.hdfs.HdfsObjectStore;
import org.trustedanalytics.store.hdfs.KerberosClientConfiguration;
import org.trustedanalytics.store.hdfs.OrgSpecificHdfsObjectStoreFactory;
import org.trustedanalytics.store.hdfs.fs.ApacheFileSystemFactory;
import org.trustedanalytics.store.hdfs.fs.MultiTenantFileSystemFactory;
import org.trustedanalytics.store.hdfs.fs.OAuthSecuredFileSystemFactory;
import org.trustedanalytics.store.hdfs.fs.SingleTenantFileSystemFactory;
import org.trustedanalytics.store.s3.S3ObjectStore;
import org.trustedanalytics.store.s3.S3ServiceInfo;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.Cloud;
import org.springframework.cloud.CloudFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@org.springframework.context.annotation.Configuration
@EnableConfigurationProperties({HdfsProperties.class, KerberosClientConfiguration.class})
public class ObjectStoreConfiguration {

    private static final String KERBEROS_SERVICE_NAME = "kerberos-service";

    @Autowired
    private HdfsProperties hdfsProps;
    
    @Autowired
    private KerberosClientConfiguration krbProps;
    
    @Bean
    @Profile("default")
    public ObjectStore objectStore() {
        return new InFolderObjectStore(System.getenv("DOWNLOADS_DIR"));
    }

    @Bean
    @Profile("s3")
    public ObjectStore s3ObjectStore() {
        CloudFactory cloudFactory = new CloudFactory();
        Cloud cloud = cloudFactory.getCloud();
        S3ServiceInfo s3ServiceInfo = (S3ServiceInfo) cloud.getServiceInfo("S3-serv-instance");
        AWSCredentials awsCredentials =
                new BasicAWSCredentials(s3ServiceInfo.getAccessKey(), s3ServiceInfo.getSecretKey());
        AmazonS3 amazonS3 = new AmazonS3Client(awsCredentials);
        return new S3ObjectStore(amazonS3, s3ServiceInfo.getBucket());
    }

    @Bean
    @Profile("hdfs")
    public ObjectStore hdfsObjectStore() throws IOException, InterruptedException, URISyntaxException, LoginException {
        SingleTenantFileSystemFactory fsFactory =
                new SingleTenantFileSystemFactory(Configurations.newInstanceFromEnv());
        FileSystem fs = fsFactory.getFileSystem();
        Path path = fsFactory.getChrootedPath();
        return new HdfsObjectStore(fs, path);
    }

    //autowire this, if you want token to be automatically acquired from web context
    @Bean
    @Profile("multitenant-hdfs")
    public ObjectStoreFactory<UUID> multitenantHdfsObjectStoreSupplier(OrgSpecificHdfsObjectStoreFactory osFactory) throws IOException {
        return osFactory::create;
    }

    //autowire this, if you want to provide token with your own logic
    @Bean
    @Profile("multitenant-hdfs")
    public TokenizedObjectStoreFactory<UUID, String> multitenantHdfsObjectStoreFactory(OrgSpecificHdfsObjectStoreFactory osFactory)
            throws IOException {
        return osFactory::create;
    }

    @Bean
    @Profile("cloud")
    public OrgSpecificHdfsObjectStoreFactory getOSFactoryCloudfoundryVersion() throws IOException {
      AppConfiguration appConfiguration = Configurations.newInstanceFromEnv();
      ServiceInstanceConfiguration hdfsConf = appConfiguration.getServiceConfig(ServiceType.HDFS_TYPE);
      ServiceInstanceConfiguration krbConf = appConfiguration.getServiceConfig(KERBEROS_SERVICE_NAME);
      OAuthSecuredFileSystemFactory fileSystemFactory =
              new MultiTenantFileSystemFactory(hdfsConf, krbConf, new TapOAuthKerberosClient(),
                      new ApacheFileSystemFactory());
      return new OrgSpecificHdfsObjectStoreFactory(fileSystemFactory, krbConf);
    }

    @Bean
    @Profile("kubernetes")
    public OrgSpecificHdfsObjectStoreFactory getOSFactory(ServiceInstanceConfiguration hdfsConfig) throws IOException {
        OAuthSecuredFileSystemFactory fileSystemFactory =
                new MultiTenantFileSystemFactory(hdfsConfig, hdfsConfig, new TapOAuthKerberosClient(),
                        new ApacheFileSystemFactory());
        return new OrgSpecificHdfsObjectStoreFactory(fileSystemFactory, hdfsConfig);
    }

    @Bean
    @Profile("kubernetes")
    public ServiceInstanceConfiguration hdfsConfig() throws IOException {
      Map<Property, String> properties = new HashMap<>();
      properties.put(Property.HDFS_URI, hdfsProps.getUri());
      properties.put(Property.KRB_KDC, krbProps.getKdc());
      properties.put(Property.KRB_REALM, krbProps.getRealm());
      properties.put(Property.USER, krbProps.getUser());
      
      return new SimpleInstanceConfiguration("config", getHadoopConfiguration(hdfsProps.getConfigDir()), properties);
    }
    
    private static Configuration getHadoopConfiguration(String confDir) throws IOException {
      return Arrays.asList("core-site.xml", "hdfs-site.xml").stream()
        .collect(Configuration::new, (c, f) -> c.addResource(new Path(confDir + f)), (c, d) -> {});
    }
}
