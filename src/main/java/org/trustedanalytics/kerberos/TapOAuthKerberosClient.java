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
package org.trustedanalytics.kerberos;

import org.trustedanalytics.hadoop.config.client.Property;
import org.trustedanalytics.hadoop.config.client.ServiceInstanceConfiguration;
import org.trustedanalytics.hadoop.config.client.oauth.TapOauthToken;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManager;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManagerFactory;

import org.apache.hadoop.conf.Configuration;

import javax.security.auth.login.LoginException;
import java.io.IOException;

public class TapOAuthKerberosClient implements OAuthKerberosClient {

    private static final String KERBEROS_AUTHENTICATION_METHOD = "kerberos";
    private static final String AUTHENTICATION_METHOD_PROPERTY = "hadoop.security.authentication";

    @Override
    public void loginIfKerberosEnabled(Configuration hadoopConf, ServiceInstanceConfiguration krbConf,
            TapOauthToken jwtToken) throws LoginException, IOException {

        if (isKerberosEnabled(hadoopConf)) {
            String kdc = krbConf.getProperty(Property.KRB_KDC).get();
            String realm = krbConf.getProperty(Property.KRB_REALM).get();
            KrbLoginManager loginManager = KrbLoginManagerFactory.getInstance().getKrbLoginManagerInstance(kdc, realm);
            loginManager.loginInHadoop(loginManager.loginWithJWTtoken(jwtToken), hadoopConf);
        }
    }

    private boolean isKerberosEnabled(Configuration hadoopConf) {
        return KERBEROS_AUTHENTICATION_METHOD.equals(hadoopConf.get(AUTHENTICATION_METHOD_PROPERTY));
    }
}
