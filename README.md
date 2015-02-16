ElasticLib - A distributed digital library
==========================================

## Overview

ElasticLib is a digital library. It enables you to store all your documents, music, pictures, videos and more.
Its primary goal is to simplify or even to get rid of the tedious tasks associated with organizing your digital life, mainly:

* Sorting files by folders.
* Retrieving a given file.
* Making backups.

ElasticLib lets you create repositories. Each repository is a place where you can store any type of content.
All files in a repository are automatically sorted and indexed, which means that:

* You no longer have to sort your files by folders, just put them in a repository.
* You can search and retrieve files by keywords, like you would do with a search engine on the web.

Concerning this latter point, metadata is automatically extracted from each file stored in a repository.

* For an audio file, it could be the author, title, album...
* For a photo, the date on which it was taken, its format and size.
* Content is also extracted from readable files like PDFs or flat text ones.

All collected metadata is stored along with associated file in the repository and indexed in an embedded search engine,
so you can find files by searching on metadata or text content. Metadata is also freely modifiable, in order to enable you to
enhance or correct information about a given file.

Instead of having to make backups of your files, elasticLib lets you synchronize repositories. You just have to
set up a replication between two repositories and any past or future change on one of them will eventually be
propagated to the other. To be more precise, replications can be:

* Unidirectional, meaning that changes are only propagated one way.
* Bidirectional, meaning that changes are propagated both ways.

The repositories are managed by nodes. A node runs on a given computer and lets you:

* Create repositories on this computer.
* Put or delete files in any repository managed by this node, and edit associated metadata.
* Search and retrieve files in a given repository.
* Set up replications between repositories of this node or those of remote nodes on other computers.

In order to accomplish that, each node expose a RESTful API. A command-line interface enables you to interact with this API.
A web-based interface is also planned.



## System requirements

* Java 8
* Works on Unix-like operating systems and Windows.
* You also need to have Maven if you would like to build it from source.
* And that's all!



## Installing

Simply unzip the ElasticLib release archive to the installation folder of your choice. 
You may want to add the `<install_dir>/elasticlib/bin/` directory to your PATH.

If you want to build ElasticLib from source, first clone the ElasticLib GitHub repository on your local computer. 
Cd into cloned root directory and run a Maven build:

```
$ mvn install
```

After the build has completed, the release archive will be available in the target directory of the distribution sub-module,
that is `elasticlib-distribution/target/elasticlib.zip`.



## Using

### Unix

Start a node on local host by typing:

```
$ elasticlibd start
```

Then, in order to begin a console session, type:

```
$ elasticlib
```

### Windows

On windows, you first have to install an ElasticLib node as a service. To this end, type:

```
> elasticlibd.cmd install
```

After that, start the installed service by typing:

```
> elasticlibd.cmd start
```

Then, in order to begin a console session, type:

```
> elasticlib.cmd
```

Please consult the [ElasticLib wiki](https://github.com/elasticlib/elasticlib/wiki) for further information.



## Roadmap


This is still a work in progress. A substantial set of the core functionalities is already implemented, which is mainly:

* A basic implementation of the main operations on a repository.
* Local replications between repositories on a given node.
* Peer-to-peer discovery mechanisms between nodes.
* A command line interface.

Some features to be done:

* Requests routing and replications between nodes.
* A web-based interface.
* Confidentiality and acces control.
* More advanced features like filtered replications, customizable indexation...
* Enhancement of existing functionalities.



## Contributing

Any contribution is welcome! Please consult the [contributing page on the wiki](https://github.com/elasticlib/elasticlib/wiki/Contributing) for further information.



## Copyright and license

Copyright 2014 Guillaume Masclet

ElasticLib is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

