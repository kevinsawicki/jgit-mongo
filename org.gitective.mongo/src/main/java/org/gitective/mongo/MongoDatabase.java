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

import static org.gitective.mongo.ICollectionConstants.CHUNKS;
import static org.gitective.mongo.ICollectionConstants.OBJECTS;
import static org.gitective.mongo.ICollectionConstants.REFS;
import static org.gitective.mongo.ICollectionConstants.REPOS;
import static org.gitective.mongo.ICollectionConstants.REPO_INDEX;
import static org.gitective.mongo.ICollectionConstants.REPO_INFO;

import com.mongodb.DB;

import java.io.IOException;

import org.eclipse.jgit.storage.dht.DhtRepository;
import org.eclipse.jgit.storage.dht.DhtRepositoryBuilder;
import org.eclipse.jgit.storage.dht.spi.ChunkTable;
import org.eclipse.jgit.storage.dht.spi.Database;
import org.eclipse.jgit.storage.dht.spi.ObjectIndexTable;
import org.eclipse.jgit.storage.dht.spi.RefTable;
import org.eclipse.jgit.storage.dht.spi.RepositoryIndexTable;
import org.eclipse.jgit.storage.dht.spi.RepositoryTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;
import org.eclipse.jgit.util.FS;

/**
 * MongoDB-backed Git database
 */
public class MongoDatabase implements Database {

	/**
	 * Open repository on database with given name
	 * 
	 * @param name
	 * @return repository
	 * @throws IOException
	 */
	public static DhtRepository open(String name) throws IOException {
		return open(new MongoDatabase(), name);
	}

	/**
	 * Open repository on given database
	 * 
	 * @param db
	 * @param name
	 * @return repository
	 * @throws IOException
	 */
	public static DhtRepository open(MongoDatabase db, String name)
			throws IOException {
		@SuppressWarnings("rawtypes")
		DhtRepositoryBuilder<DhtRepositoryBuilder, DhtRepository, MongoDatabase> builder = new DhtRepositoryBuilder<DhtRepositoryBuilder, DhtRepository, MongoDatabase>();
		builder.setDatabase(db);
		builder.setRepositoryName(name);
		builder.setMustExist(false);
		builder.setFS(FS.DETECTED);
		return builder.build();
	}

	private final MongoRepositoryIndexTable repositoryIndex;

	private final MongoRepositoryTable repository;

	private final MongoRefTable ref;

	private final MongoObjectIndexTable objectIndex;

	private final MongoChunkTable chunk;

	/**
	 * Create a MongoDB-backed database
	 * 
	 * @param db
	 */
	public MongoDatabase(final DB db) {
		repositoryIndex = new MongoRepositoryIndexTable(
				db.getCollection(REPO_INDEX));

		repository = new MongoRepositoryTable(db.getCollection(REPOS),
				db.getCollection(REPO_INFO));

		ref = new MongoRefTable(db.getCollection(REFS));

		objectIndex = new MongoObjectIndexTable(db.getCollection(OBJECTS));

		chunk = new MongoChunkTable(db.getCollection(CHUNKS));
	}

	/**
	 * Create a MongoDB-backed database
	 * 
	 * @param dbName
	 */
	public MongoDatabase(final String dbName) {
		this(MongoUtils.getDB(dbName));
	}

	/**
	 * Create a MongoDB-backed database with database name of 'git'
	 */
	public MongoDatabase() {
		this("git");
	}

	public RepositoryIndexTable repositoryIndex() {
		return repositoryIndex;
	}

	public RepositoryTable repository() {
		return repository;
	}

	public RefTable ref() {
		return ref;
	}

	public ObjectIndexTable objectIndex() {
		return objectIndex;
	}

	public ChunkTable chunk() {
		return chunk;
	}

	public WriteBuffer newWriteBuffer() {
		return new MongoWriteBuffer();
	}
}
