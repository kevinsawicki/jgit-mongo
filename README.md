# JGit MongoDB connector

This project is a [JGit](http://www.eclipse.org/jgit/) DHT implementation using
[MongoDB](http://www.mongodb.org/) as the backing database that uses the
[Mongo Java Driver](https://github.com/mongodb/mongo-java-driver) library for
connecting to MongoDB.

## Example

The code snippet below shows how to fetch the linux-2.6 Git repository into
MongoDB using a JGit repository.

```java
Repository repo = MongoDatabase.open("linux-26");
repo.create(true);
StoredConfig config = repo.getConfig();
RemoteConfig remoteConfig = new RemoteConfig(config, "origin");
URIish uri = new URIish("git://github.com/mirrors/linux-2.6.git");
remoteConfig.addURI(uri);
remoteConfig.update(config);
config.save();
RefSpec spec = new RefSpec("refs/heads/*:refs/remotes/origin/*");
Git.wrap(repo).fetch().setRemote("origin").setRefSpecs(spec).call();
```

You could the walk the commits imported into MongoDB using the following:

```java
Repository repo = MongoDatabase.open("linux-26");
RevWalk walk = new RevWalk(repo);
walk.markStart(walk.parseCommit(repo.resolve("origin/master")));
for (RevCommit commit : walk)
    System.out.println(commit.getShortMessage());
```

## Building from source
The JGit-MongoDB connector can be built using [Maven](http://maven.apache.org/).
The pom.xml to build the core plug-in is located at the root of the org.gitective.mongo folder.

```
cd jgit-mongo/org.gitective.mongo
mvn clean install
```

## Dependencies

* JGit 1.0+
* MongoDB Java Driver 2.6.3+

## License

[MIT License](http://www.opensource.org/licenses/mit-license.php)
