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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.authentication.OAuth2AuthenticationDetails;
import org.trustedanalytics.hadoop.config.client.AppConfiguration;
import org.trustedanalytics.hadoop.config.client.Configurations;
import org.trustedanalytics.hadoop.config.client.Property;
import org.trustedanalytics.hadoop.config.client.ServiceInstanceConfiguration;
import org.trustedanalytics.hadoop.config.client.ServiceType;
import org.trustedanalytics.hadoop.config.client.oauth.TapOauthToken;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManager;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManagerFactory;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.UUID;

public class OrgSpecificHdfsObjectStoreFactory {

    private final ServiceInstanceConfiguration hdfsConf;
    private final ServiceInstanceConfiguration krbConf;
    private final String cfUser;
    private final String hiveUser;

    public OrgSpecificHdfsObjectStoreFactory() throws IOException {
        AppConfiguration appConfiguration = Configurations.newInstanceFromEnv();
        hdfsConf = appConfiguration.getServiceConfig(ServiceType.HDFS_TYPE);
        krbConf  = appConfiguration.getServiceConfig("kerberos-service");
        cfUser = krbConf.getProperty(Property.USER).orElse("cf");
        hiveUser = Optional.ofNullable(System.getenv("HIVE_TECHNICAL_USER")).orElse("hive");
    }

    @VisibleForTesting
    OrgSpecificHdfsObjectStoreFactory(AppConfiguration appConfiguration) throws IOException {
        hdfsConf = appConfiguration.getServiceConfig(ServiceType.HDFS_TYPE);
        krbConf  = appConfiguration.getServiceConfig("kerberos-service");
        cfUser = krbConf.getProperty(Property.USER).orElse("cf");
        hiveUser = Optional.ofNullable(System.getenv("HIVE_TECHNICAL_USER")).orElse("hive");
    }

    public OrgSpecificHdfsObjectStore create(UUID org) {
        return create(org, getOAuthToken());
    }

    private String getOAuthToken() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        OAuth2Authentication oauth2 = (OAuth2Authentication) auth;
        OAuth2AuthenticationDetails details = (OAuth2AuthenticationDetails) oauth2.getDetails();
        return details.getTokenValue();
    }

    public OrgSpecificHdfsObjectStore create(UUID org, String OAuthToken) {
        try {
            return new OrgSpecificHdfsObjectStore(cfUser, hiveUser, getFileSystem(OAuthToken, org), getHdfsUri(org));
        } catch (IOException e) {
            //TODO: must be thrown as 500 (?)
            e.printStackTrace();
        }
        return null; //TODO: remove, when you rethrow IOException
    }

    private FileSystem getFileSystem(String token, UUID org) {
        try {

            //TODO START: this code is from hadoop-utils (95%),
            // we need to change hadoop-utils to enable uri with templates like hdfs://name/org/%{organization}/catalog
            TapOauthToken jwtToken = new TapOauthToken(token);
            Configuration hadoopConf = hdfsConf.asHadoopConfiguration();

            if (isKerberosEnabled(hadoopConf)) {
                String kdc = krbConf.getProperty(Property.KRB_KDC).get();
                String realm = krbConf.getProperty(Property.KRB_REALM).get();
                KrbLoginManager loginManager =
                    KrbLoginManagerFactory.getInstance().getKrbLoginManagerInstance(kdc, realm);
                loginManager.loginInHadoop(loginManager.loginWithJWTtoken(jwtToken), hadoopConf);
            }

            URI uri = new URI(getHdfsUri(org));
            return FileSystem.get(uri, hadoopConf, jwtToken.getUserId());
            //TODO END

        } catch (LoginException e) {
            //TODO: must be thrown as 401
            e.printStackTrace();
        } catch (IOException | InterruptedException | URISyntaxException e) {
            //TODO: must be thrown as 500
            e.printStackTrace();
        }
        return null; //TODO: remove, when you rethrow IOException
    }

    private static final String AUTHENTICATION_METHOD = "kerberos";
    private static final String AUTHENTICATION_METHOD_PROPERTY = "hadoop.security.authentication";
    private boolean isKerberosEnabled(Configuration hadoopConf) {
        return AUTHENTICATION_METHOD.equals(hadoopConf.get(AUTHENTICATION_METHOD_PROPERTY));
    }

    @VisibleForTesting
    String getHdfsUri(UUID org) {
        return PathTemplate.resolveOrg(hdfsConf.getProperty(Property.HDFS_URI).get(), org);
    }

    private static class PathTemplate {
        private static final String ORG_PLACEHOLDER = "organization";
        private static final String PLACEHOLDER_PREFIX = "%{";
        private static final String PLACEHOLDER_SUFIX = "}";

        private static String resolveOrg(String url, UUID org) {
            ImmutableMap<String, UUID> values = ImmutableMap.of(ORG_PLACEHOLDER, org);
            return new StrSubstitutor(values, PLACEHOLDER_PREFIX, PLACEHOLDER_SUFIX).replace(url);
        }
    }
}
