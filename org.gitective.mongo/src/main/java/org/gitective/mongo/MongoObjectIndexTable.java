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

import static org.gitective.mongo.IPropertyConstants.VALUES;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.eclipse.jgit.generated.storage.dht.proto.GitStore;
import org.eclipse.jgit.storage.dht.AsyncCallback;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.ObjectIndexKey;
import org.eclipse.jgit.storage.dht.ObjectInfo;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.ObjectIndexTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

/**
 * MongoDB-backed object index table
 */
public class MongoObjectIndexTable implements ObjectIndexTable {

	private static final String VALUES_PREFIX = VALUES + ".";

	private final DBCollection collection;

	/**
	 * 
	 * @param collection
	 */
	public MongoObjectIndexTable(DBCollection collection) {
		this.collection = collection;
	}

	public void get(Context options, Set<ObjectIndexKey> objects,
			AsyncCallback<Map<ObjectIndexKey, Collection<ObjectInfo>>> callback) {
		Map<ObjectIndexKey, Collection<ObjectInfo>> out = new HashMap<ObjectIndexKey, Collection<ObjectInfo>>();
		for (ObjectIndexKey objId : objects) {
			DBObject fetch = collection.findOne(new IdObject(objId.asString()));
			if (fetch == null)
				continue;
			Object values = fetch.get(VALUES);
			if (values instanceof DBObject) {
				DBObject dbo = (DBObject) values;
				Collection<ObjectInfo> chunks = out.get(objId);
				if (chunks == null) {
					chunks = new ArrayList<ObjectInfo>(4);
					out.put(objId, chunks);
				}
				for (String key : dbo.keySet()) {
					byte[] value = MongoUtils.getBytes(dbo, key);
					key = unescapeKey(key);
					try {
						chunks.add(new ObjectInfo(ChunkKey.fromString(key), 0,
								GitStore.ObjectInfo.parseFrom(value)));
					} catch (InvalidProtocolBufferException e) {
						callback.onFailure(new DhtException(e));
						return;
					}
				}
			}
		}
		callback.onSuccess(out);
	}

	private String escapeKey(String key) {
		return key.replace('.', ':');
	}

	private String unescapeKey(String key) {
		return key.replace(':', '.');
	}

	public void add(ObjectIndexKey objId, ObjectInfo info, WriteBuffer buffer)
			throws DhtException {
		IdObject id = new IdObject(objId.asString());
		String key = escapeKey(info.getChunkKey().asString());
		DBObject object = MongoUtils.set(VALUES_PREFIX + key, info.getData()
				.toByteArray());
		collection.update(id, object, true, false);
	}

	public void remove(ObjectIndexKey objId, ChunkKey chunk, WriteBuffer buffer)
			throws DhtException {
		IdObject id = new IdObject(objId.asString());
		String key = escapeKey(chunk.asString());
		collection.update(id, MongoUtils.unset(VALUES_PREFIX + key), true,
				false);
	}
}
