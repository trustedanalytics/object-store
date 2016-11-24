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

import org.trustedanalytics.hadoop.config.client.Property;
import org.trustedanalytics.hadoop.config.client.ServiceInstanceConfiguration;
import org.trustedanalytics.store.hdfs.fs.OAuthSecuredFileSystemFactory;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.authentication.OAuth2AuthenticationDetails;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

public class OrgSpecificHdfsObjectStoreFactory {

    private static final String CF_DEFAULT_USER = "cf";
    private static final String HIVE_USER_ENV_VARIABLE_NAME = "HIVE_TECHNICAL_USER";
    private static final String HIVE_DEFAULT_USER = "hive";
    private static final String ARCADIA_USER_ENV_VARIABLE_NAME = "ARCADIA_TECHNICAL_USER";
    private static final String ARCADIA_DEFAULT_USER = "arcadia-user";
    private static final String VCAP_USER_ENV_VARIABLE_NAME = "vcap_TECHNICAL_USER";
    private static final String VCAP_DEFAULT_USER = "vcap";

    private final OAuthSecuredFileSystemFactory fileSystemFactory;
    private final ImmutableList<String> technicalUsers;

    public OrgSpecificHdfsObjectStoreFactory(OAuthSecuredFileSystemFactory fileSystemFactory,
            ServiceInstanceConfiguration krbConf) throws IOException {

        this.fileSystemFactory = fileSystemFactory;
        this.technicalUsers = getTechnicalUsers(krbConf);
    }

    private ImmutableList<String> getTechnicalUsers(ServiceInstanceConfiguration krbConf) {
        String cfUser = krbConf.getProperty(Property.USER)
            .orElse(CF_DEFAULT_USER);
        String hiveUser = Optional.ofNullable(System.getenv(HIVE_USER_ENV_VARIABLE_NAME))
            .orElse(HIVE_DEFAULT_USER);
        String arcadiaUser = Optional.ofNullable(System.getenv(ARCADIA_USER_ENV_VARIABLE_NAME))
            .orElse(ARCADIA_DEFAULT_USER);
        String vcapUser = Optional.ofNullable(System.getenv(VCAP_USER_ENV_VARIABLE_NAME))
            .orElse(VCAP_DEFAULT_USER);
        return ImmutableList.of(cfUser, hiveUser, arcadiaUser, vcapUser);
    }

    public OrgSpecificHdfsObjectStore create(UUID org) throws IOException, InterruptedException, LoginException {
        return create(org, getOAuthToken());
    }

    public OrgSpecificHdfsObjectStore create(UUID org, String oAuthToken)
            throws IOException, InterruptedException, LoginException {
        FileSystem fs = fileSystemFactory.getFileSystem(oAuthToken);
        String uri = fileSystemFactory.getHdfsUri(org);
        return new OrgSpecificHdfsObjectStore(technicalUsers, fs, uri);
    }

    private String getOAuthToken() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        OAuth2Authentication oauth2 = (OAuth2Authentication) auth;
        OAuth2AuthenticationDetails details = (OAuth2AuthenticationDetails) oauth2.getDetails();
        return details.getTokenValue();
    }
}
