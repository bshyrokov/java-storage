/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.storage.it;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.paging.Page;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketFixture;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.StorageFixture;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.conformance.retry.CleanupStrategy;
import com.google.cloud.storage.conformance.retry.ParallelParameterized;
import com.google.cloud.storage.conformance.retry.TestBench;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(ParallelParameterized.class)
public final class ITGrpcTest {
  private static final Logger LOGGER = Logger.getLogger(ITGrpcTest.class.getName());
  @ClassRule(order = 1)
  public static final TestBench TEST_BENCH =
      TestBench.newBuilder().setContainerName("it-grpc").setDockerImageTag("v0.26.0").build();

  @Rule
  public final StorageFixture storageFixture;

  @Rule
  public final BucketFixture bucketFixture;

  public ITGrpcTest(StorageFixture storageFixture) {
    this.storageFixture = storageFixture;
    this.bucketFixture = BucketFixture.newBuilder()
        .setBucketNameFmtString("java-storage-gcs-grpc-team-%s")
        .setCleanupStrategy(CleanupStrategy.ALWAYS)
        .setHandle(storageFixture::getInstance)
        .build();
  }

  @Parameters( name = "{0}")
  public static Collection<StorageFixture> data() {
    StorageFixture grpcStorageFixture = StorageFixture.from(
        () ->
            StorageOptions.grpc()
                .setHost(TEST_BENCH.getGRPCBaseUri())
                .setCredentials(NoCredentials.getInstance())
                .setProjectId("test-project-id")
                .build());
    StorageFixture jsonStorageFixture = StorageFixture.defaultHttp();
    return Arrays.asList(grpcStorageFixture, jsonStorageFixture);
  }

  @Test
  public void testCreateBucket() {
    LOGGER.info("Running testCreateBucket with " + storageFixture.getInstance().getOptions().getHost());
    final String bucketName = RemoteStorageHelper.generateBucketName();
    Bucket bucket = storageFixture.getInstance().create(BucketInfo.of(bucketName));
    assertThat(bucket.getName()).isEqualTo(bucketName);
  }

  @Test
  public void listBlobs() {
    LOGGER.info("Running listBlobs with " + storageFixture.getInstance().getOptions().getHost());
    BucketInfo bucketInfo = bucketFixture.getBucketInfo();
    Page<Blob> list = storageFixture.getInstance().list(bucketInfo.getName());
    ImmutableList<String> bucketNames =
        StreamSupport.stream(list.iterateAll().spliterator(), false)
            .map(Blob::getName)
            .collect(ImmutableList.toImmutableList());

    assertThat(bucketNames).isEmpty();
  }

  @Test
  public void listBuckets() {
    LOGGER.info("Running listBuckets with " + storageFixture.getInstance().getOptions().getHost());
    Page<Bucket> list = storageFixture.getInstance().list();
    ImmutableList<String> bucketNames =
        StreamSupport.stream(list.iterateAll().spliterator(), false)
            .map(Bucket::getName)
            .collect(ImmutableList.toImmutableList());

    assertThat(bucketNames).contains(bucketFixture.getBucketInfo().getName());
  }
}
