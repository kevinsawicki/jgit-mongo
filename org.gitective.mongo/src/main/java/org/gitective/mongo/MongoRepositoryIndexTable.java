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

import static org.gitective.mongo.IPropertyConstants.ID;
import static org.gitective.mongo.IPropertyConstants.NAME;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.util.concurrent.TimeoutException;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.RepositoryName;
import org.eclipse.jgit.storage.dht.spi.RepositoryIndexTable;

/**
 * MongoDB-backed repository index table
 */
public class MongoRepositoryIndexTable implements RepositoryIndexTable {

	private final DBCollection collection;

	/**
	 * @param collection
	 * 
	 */
	public MongoRepositoryIndexTable(DBCollection collection) {
		this.collection = collection;
	}

	public RepositoryKey get(RepositoryName name) throws DhtException,
			TimeoutException {
		DBObject value = collection.findOne(new BasicDBObject(NAME, name
				.asString()));
		if (value == null)
			return null;
		return RepositoryKey.fromInt(MongoUtils.getInt(value, ID));
	}

	public void putUnique(RepositoryName name, RepositoryKey key)
			throws DhtException, TimeoutException {
		BasicDBObject repo = MongoUtils.set(NAME, name.asString());
		collection.update(new IdObject(key.asInt()), repo, true, false);
	}

	public void remove(RepositoryName name, RepositoryKey key)
			throws DhtException, TimeoutException {
		IdObject id = new IdObject(key.asInt());
		DBObject current = collection.findOne(id);
		if (current == null)
			return;
		String currentName = MongoUtils.getString(current, NAME);
		if (currentName == null || !currentName.equals(name.asString()))
			return;
		collection.update(id, MongoUtils.unset(NAME), true, false);
	}
}
