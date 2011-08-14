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
import static org.gitective.mongo.IPropertyConstants.INDEX;
import static org.gitective.mongo.IPropertyConstants.META;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.jgit.generated.storage.dht.proto.GitStore.ChunkMeta;
import org.eclipse.jgit.storage.dht.AsyncCallback;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.PackChunk.Members;
import org.eclipse.jgit.storage.dht.spi.ChunkTable;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

/**
 * MongoDB-backed chunk table
 */
public class MongoChunkTable implements ChunkTable {

	private final DBCollection collection;

	/**
	 * @param collection
	 */
	public MongoChunkTable(DBCollection collection) {
		this.collection = collection;
	}

	public void get(Context options, Set<ChunkKey> keys,
			AsyncCallback<Collection<Members>> callback) {
		List<Members> out = new ArrayList<Members>(keys.size());
		for (ChunkKey chunk : keys) {
			DBObject value = collection.findOne(new IdObject(chunk.asString()));
			if (value == null)
				continue;
			byte[] buffer = MongoUtils.getBytes(value, DATA);
			if (buffer == null)
				continue;

			Members members = new Members();
			members.setChunkKey(chunk);
			members.setChunkData(buffer);

			buffer = MongoUtils.getBytes(value, INDEX);
			if (buffer != null)
				members.setChunkIndex(buffer);

			buffer = MongoUtils.getBytes(value, META);
			if (buffer != null)
				try {
					members.setMeta(ChunkMeta.parseFrom(buffer));
				} catch (InvalidProtocolBufferException e) {
					callback.onFailure(new DhtException(e));
					return;
				}
			out.add(members);
		}
		callback.onSuccess(out);
	}

	public void getMeta(Context options, Set<ChunkKey> keys,
			AsyncCallback<Map<ChunkKey, ChunkMeta>> callback) {
		Map<ChunkKey, ChunkMeta> out = new HashMap<ChunkKey, ChunkMeta>();
		try {
			for (ChunkKey chunk : keys) {
				DBObject object = collection.findOne(new IdObject(chunk
						.asString()));
				if (object == null)
					continue;
				byte[] value = MongoUtils.getBytes(object, META);
				if (value != null)
					out.put(chunk, ChunkMeta.parseFrom(value));
			}
			callback.onSuccess(out);
		} catch (InvalidProtocolBufferException e) {
			callback.onFailure(new DhtException(e));
		}
	}

	/**
	 * Update/insert chunk into collection
	 * 
	 * @param query
	 * @param document
	 */
	protected void upsert(DBObject query, DBObject document) {
		collection.update(query, document, true, false);
	}

	public void put(Members chunk, WriteBuffer buffer) throws DhtException {
		final IdObject id = new IdObject(chunk.getChunkKey().asString());

		if (chunk.hasChunkData())
			upsert(id, MongoUtils.set(DATA, chunk.getChunkData()));

		if (chunk.hasChunkIndex())
			upsert(id, MongoUtils.set(INDEX, chunk.getChunkIndex()));

		if (chunk.hasMeta())
			upsert(id, MongoUtils.set(META, chunk.getMeta().toByteArray()));
	}

	public void remove(ChunkKey key, WriteBuffer buffer) throws DhtException {
		collection.remove(new IdObject(key.asString()));
	}
}
