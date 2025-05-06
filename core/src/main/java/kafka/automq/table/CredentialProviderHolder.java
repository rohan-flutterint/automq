/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.automq.table;
import com.automq.stream.s3.operator.BucketURI;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import static com.automq.stream.s3.operator.AwsObjectStorage.AUTH_TYPE_KEY;
import static com.automq.stream.s3.operator.AwsObjectStorage.INSTANCE_AUTH_TYPE;
import static com.automq.stream.s3.operator.AwsObjectStorage.STATIC_AUTH_TYPE;

public class CredentialProviderHolder implements AwsCredentialsProvider {
    private static AwsCredentialsProvider provider;

    public static void setup(AwsCredentialsProvider provider) {
        CredentialProviderHolder.provider = provider;
    }

    public static void setup(BucketURI bucketURI) {
        CredentialProviderHolder.provider = newCredentialsProviderChain(credentialsProviders(bucketURI));
    }

    private static List<AwsCredentialsProvider> credentialsProviders(BucketURI bucketURI) {
        String authType = bucketURI.extensionString(AUTH_TYPE_KEY, STATIC_AUTH_TYPE);
        switch (authType) {
            case STATIC_AUTH_TYPE: {
                String accessKey = bucketURI.extensionString(BucketURI.ACCESS_KEY_KEY, System.getenv("KAFKA_S3_ACCESS_KEY"));
                String secretKey = bucketURI.extensionString(BucketURI.SECRET_KEY_KEY, System.getenv("KAFKA_S3_SECRET_KEY"));
                if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)) {
                    return Collections.emptyList();
                }
                return List.of(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
            }
            case INSTANCE_AUTH_TYPE: {
                AwsCredentialsProvider instanceProfileCredentialsProvider;
                switch (bucketURI.protocol()) {
                    case "s3":
                        instanceProfileCredentialsProvider = InstanceProfileCredentialsProvider.builder().build();
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported protocol: " + bucketURI.protocol());
                }
                return List.of(instanceProfileCredentialsProvider);
            }
            default:
                throw new UnsupportedOperationException("Unsupported auth type: " + authType);
        }
    }

    private static AwsCredentialsProvider newCredentialsProviderChain(
        List<AwsCredentialsProvider> credentialsProviders) {
        List<AwsCredentialsProvider> providers = new ArrayList<>(credentialsProviders);
        // Add default providers to the end of the chain
        providers.add(InstanceProfileCredentialsProvider.create());
        providers.add(AnonymousCredentialsProvider.create());
        return AwsCredentialsProviderChain.builder()
            .reuseLastProviderEnabled(true)
            .credentialsProviders(providers)
            .build();
    }

    // iceberg will invoke create with reflection.
    public static AwsCredentialsProvider create() {
        return provider;
    }

    @Override
    public AwsCredentials resolveCredentials() {
        throw new UnsupportedOperationException();
    }
}
