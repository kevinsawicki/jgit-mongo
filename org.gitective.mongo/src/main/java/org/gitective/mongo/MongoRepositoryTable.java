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

import static org.gitective.mongo.IPropertyConstants.CHUNKS;
import static org.gitective.mongo.IPropertyConstants.KEY;
import static org.gitective.mongo.IPropertyConstants.METADATA;
import static org.gitective.mongo.IPropertyConstants.PACKS;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.eclipse.jgit.generated.storage.dht.proto.GitStore.CachedPackInfo;
import org.eclipse.jgit.storage.dht.CachedPackKey;
import org.eclipse.jgit.storage.dht.ChunkInfo;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.spi.RepositoryTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

/**
 * MongoDB-backed repository table
 */
public class MongoRepositoryTable implements RepositoryTable {

	private static final String PACKS_PREFIX = PACKS + ".";

	private static final String CHUNKS_PREFIX = CHUNKS + ".";

	private final DBCollection collection;

	private final DBCollection metadata;

	/**
	 * @param collection
	 * @param metadata
	 */
	public MongoRepositoryTable(DBCollection collection, DBCollection metadata) {
		this.collection = collection;
		this.metadata = metadata;
	}

	public RepositoryKey nextKey() throws DhtException {
		DBObject value = new BasicDBObject(METADATA, 0);
		DBObject updated = metadata.findAndModify(value, null, null, false,
				MongoUtils.inc(KEY), true, true);
		return RepositoryKey.fromInt(MongoUtils.getInt(updated, KEY));
	}

	public void put(RepositoryKey repo, ChunkInfo info, WriteBuffer buffer)
			throws DhtException {
		String key = CHUNKS_PREFIX + info.getChunkKey().asString();
		DBObject object = MongoUtils.set(key, info.getData().toByteArray());
		collection.update(new IdObject(repo.asInt()), object, true, false);
	}

	public void remove(RepositoryKey repo, ChunkKey chunk, WriteBuffer buffer)
			throws DhtException {
		DBObject object = MongoUtils.unset(CHUNKS_PREFIX + chunk.asString());
		collection.update(new IdObject(repo.asInt()), object, true, false);
	}

	public Collection<CachedPackInfo> getCachedPacks(RepositoryKey repo)
			throws DhtException, TimeoutException {
		DBObject object = collection.findOne(new IdObject(repo.asInt()));
		if (object == null)
			return Collections.emptyList();
		Object packs = object.get(PACKS);
		if (!(packs instanceof DBObject))
			return Collections.emptyList();
		DBObject dbo = (DBObject) packs;
		List<CachedPackInfo> info = new ArrayList<CachedPackInfo>();
		for (String key : dbo.keySet()) {
			byte[] value = MongoUtils.getBytes(dbo, key);
			try {
				info.add(CachedPackInfo.parseFrom(value));
			} catch (InvalidProtocolBufferException e) {
				throw new DhtException(e);
			}
		}
		return info;
	}

	public void put(RepositoryKey repo, CachedPackInfo info, WriteBuffer buffer)
			throws DhtException {
		CachedPackKey key = CachedPackKey.fromInfo(info);
		DBObject object = MongoUtils.set(PACKS_PREFIX + key.asString(),
				info.toByteArray());
		collection.update(new IdObject(repo.asInt()), object, true, false);
	}

	public void remove(RepositoryKey repo, CachedPackKey key, WriteBuffer buffer)
			throws DhtException {
		DBObject object = MongoUtils.unset(PACKS_PREFIX + key.asString());
		collection.update(new IdObject(repo.asInt()), object, true, false);
	}
}
