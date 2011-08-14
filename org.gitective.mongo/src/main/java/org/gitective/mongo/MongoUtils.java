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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * MongoDB helpers
 */
public class MongoUtils {

	/**
	 * Are the two given byte arrays equal?
	 * 
	 * @param old
	 * @param current
	 * @return true if equal, false if different
	 */
	public static boolean equal(byte[] old, byte[] current) {
		return (old == null && current == null) || Arrays.equals(old, current);
	}

	/**
	 * Get byte array value of key
	 * 
	 * @param object
	 * @param key
	 * @return byte array value
	 */
	public static byte[] getBytes(DBObject object, String key) {
		Object value = object.get(key);
		return value instanceof byte[] ? (byte[]) value : null;
	}

	/**
	 * Get string value of key
	 * 
	 * @param object
	 * @param key
	 * @return string value
	 */
	public static String getString(DBObject object, String key) {
		Object value = object.get(key);
		return value != null ? value.toString() : null;
	}

	/**
	 * Get integer value of key
	 * 
	 * @param object
	 * @param key
	 * @return integer value
	 */
	public static int getInt(DBObject object, String key) {
		Object value = object.get(key);
		return value instanceof Integer ? ((Integer) value) : -1;
	}

	/**
	 * Set key to value
	 * 
	 * @param key
	 * @param value
	 * @return query
	 */
	public static BasicDBObject set(String key, Object value) {
		return new BasicDBObject("$set", new BasicDBObject(key, value));
	}

	/**
	 * Unset key
	 * 
	 * @param key
	 * @return query
	 */
	public static BasicDBObject unset(String key) {
		return new BasicDBObject("$unset", new BasicDBObject(key, 1));
	}

	/**
	 * Increment key by 1
	 * 
	 * @param key
	 * @return query
	 */
	public static BasicDBObject inc(String key) {
		return inc(key, 1);
	}

	/**
	 * Increment key to by value
	 * 
	 * @param key
	 * @param value
	 * @return query
	 */
	public static BasicDBObject inc(String key, Object value) {
		return new BasicDBObject("$inc", new BasicDBObject(key, value));
	}

	/**
	 * Get DB with given name using default connection
	 * 
	 * @param name
	 * @return database
	 */
	public static DB getDB(String name) {
		try {
			return new Mongo().getDB(name);
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException(e);
		} catch (MongoException e) {
			throw new IllegalArgumentException(e);
		}
	}
}
