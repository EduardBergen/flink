/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface LibraryCacheManager {
	/**
	 * Returns the user code class loader associated with id.
	 *
	 * @param id identifying the job
	 * @return ClassLoader which can load the user code
	 */
	ClassLoader getClassLoader(JobID id);

	/**
	 * Returns a file handle to the file identified by the blob key.
	 *
	 * @param blobKey identifying the requested file
	 * @return File handle
	 * @throws IOException
	 */
	File getFile(BlobKey blobKey) throws IOException;

	/**
	 * Registers a job with its required jar files. The jar files are identified by their blob keys.
	 *
	 * @param id job ID
	 * @param requiredJarFiles collection of blob keys identifying the required jar files
	 * @throws IOException
	 */
	void registerJob(JobID id, Collection<BlobKey> requiredJarFiles) throws IOException;
	
	/**
	 * Registers a job task execution with its required jar files. The jar files are identified by their blob keys.
	 *
	 * @param id job ID
	 * @param requiredJarFiles collection of blob keys identifying the required jar files
	 * @throws IOException
	 */
	void registerTask(JobID id, ExecutionAttemptID execution, Collection<BlobKey> requiredJarFiles) throws IOException;

	/**
	 * Unregisters a job from the library cache manager.
	 *
	 * @param id job ID
	 */
	void unregisterTask(JobID id, ExecutionAttemptID execution);
	
	/**
	 * Unregisters a job from the library cache manager.
	 *
	 * @param id job ID
	 */
	void unregisterJob(JobID id);

	/**
	 * Shutdown method
	 *
	 * @throws IOException
	 */
	void shutdown() throws IOException;

	/**
	 * Returns a collection of required jar archives
	 *
	 * @param blobs identifying the requested archives of current library cache manager
	 * @return collection of jar archives
	 * @throws IOException
	 */
	List<File> GetAllRequiredJarFiles(List<BlobKey> blobs) throws IOException;

	/**
	 * Returns required job archives BlobKeys
	 *
	 * @param jobId identifying the job
	 * @return Set of BlobKeys
	 */
	Set<BlobKey> getRegisteredTaskBlobKeys(JobID jobId);

	/**
	 * Saves jar archives into the local filesystem
	 *
	 * @param manager identifying the library cache manager of requested archives
	 * @param jobId identifying the requested jar archives
	 * @param path location to store the file
	 * @return File handle
	 * @throws IOException
	 */
	void SaveJarsToFilesystem(LibraryCacheManager manager, JobID jobId, String path) throws IOException;
}
