/*
 * Copyright (c) 2011 Kevin Sawicki <kevinsawicki@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */
package org.gitective.mongo;

import static org.gitective.mongo.IPropertyConstants.DATA;
import static org.gitective.mongo.IPropertyConstants.NAME;
import static org.gitective.mongo.IPropertyConstants.REPO;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.eclipse.jgit.generated.storage.dht.proto.GitStore.RefData;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RefKey;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.RefTable;

/**
 * MongoDB-backed ref table
 */
public class MongoRefTable implements RefTable {

	private final DBCollection collection;

	/**
	 * @param collection
	 * 
	 */
	public MongoRefTable(DBCollection collection) {
		this.collection = collection;
	}

	private BasicDBObject createRepoObject(RepositoryKey key) {
		return new BasicDBObject(REPO, key.asInt());
	}

	public Map<RefKey, RefData> getAll(Context options, RepositoryKey repository)
			throws DhtException, TimeoutException {
		Map<RefKey, RefData> out = new HashMap<RefKey, RefData>();
		BasicDBObject repoQuery = createRepoObject(repository);
		for (DBObject object : collection.find(repoQuery)) {
			byte[] data = MongoUtils.getBytes(object, DATA);
			RefData parsed;
			try {
				parsed = RefData.parseFrom(data);
			} catch (InvalidProtocolBufferException e) {
				throw new DhtException(e);
			}
			String name = MongoUtils.getString(object, NAME);
			out.put(RefKey.create(repository, name), parsed);
		}
		return out;
	}

	public boolean compareAndPut(RefKey refKey, RefData oldData, RefData newData)
			throws DhtException, TimeoutException {
		String name = refKey.getName();
		BasicDBObject query = createRepoObject(refKey.getRepositoryKey());
		query.put(NAME, name);
		BasicDBObject object = createRepoObject(refKey.getRepositoryKey());
		object.put(NAME, name);
		object.put(DATA, newData.toByteArray());
		collection.update(query, object, true, false);
		return true;
	}

	public boolean compareAndRemove(RefKey refKey, RefData oldData)
			throws DhtException, TimeoutException {
		BasicDBObject query = createRepoObject(refKey.getRepositoryKey());
		query.put(NAME, refKey.getName());
		collection.remove(query);
		return true;
	}
}
