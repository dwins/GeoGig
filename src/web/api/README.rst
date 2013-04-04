*******************
GeoGit Web API
*******************

Proof-of-concept straw-man web API for a single repository.

It does:

* handle `status`, `log`, `commit`, `ls-tree`
* allow XML or JSON responses, including JSONP callbacks
* allow specification of the repository via the command line

It does **not**:

* allow concurrent use with command-line (bdb lock)
* look RESTful
* attempt to be complete or even consistent
* have tests yet

Implementation
==============

Currently using Restlet running on Jetty. This layer is relatively small and the bulk of the code
is in actual command implementation and response formatting.

Discussion
----------

The command implementation looks so similar to the CLI commands (with some duplication), it may
make sense to evaluate breaking a CLI command into 3 pieces: CLI argument parsing/binding, actual
execution, and output. This way the web api commands could provide specific implementations of
argument parsing and binding and output. The duplicated code (so far) is not large so this might
just add overhead for little gain.


Running
=======

First, build the whole project via the `parent` module.

To run the jetty server via maven, in the web module directory, run:

  mvn -o exec:java -Dexec.mainClass=org.geogit.web.Main -Dexec.args=PATH_TO_YOUR_REPO

In Servlet Container
--------------------

Build the geogit.war like this:

  mvn package

The output should tell you where the war is. Something like:

  Building war: <project-home>/geogit/src/web/target/geogit-web-0.1-SNAPSHOT.war

Deploy the war to your container and ensure one of the two points to the full
path to your repository:

* servlet parameter `repository`
* java system property `org.geogit.web.repository`

URLS
====

All endpoints respond to GET or POST operations:

|  /status
|  /log
|  /commit
|  /ls-tree
|  /updateref
|  /diff
|  /repo/manifest
|  /repo/objects/{id}
|  /repo/sendobject

Note: Unless `commit` is run with the `all` option or changes are staged using the command line,
nothing will happen. In other words, one cannot specify paths at the moment.

URL Parameters
--------------

Parameters may be provided as URL query items or as form-encoded POST body.

Notation
........

While URL query parameters and POST fields pose no restrictions on their string content, GeoGit generally requires them to meet some conditions.
For brevity and clarity, we enumerate some classes of parameter here, and reference these definitions below.

**Boolean**
  A boolean field may have the value "true" or "false".
  Other values are illegal.

**Integer**
  A field containing a whole number expressed in decimal notation.
  Some fields place constraints on the value of the number; for example a length field may be required to be non-negative.

**ObjectId**
  An object id is a unique identifier for a GeoGit history object (commit, tree, feature, etc.)
  An object id is encoded in text as 40-character hexadecimal string specifying the 20-byte SHA1 checksum of the object.

**Refspec**
  A refspec is an identifier for a GeoGit history object (commit, tree, feature, etc.)
  Refspecs may come in different forms:

  * An object id.
  * Any unique prefix of such a checksum.
  * A name from GeoGit's name database (these are used for branches and other commits of special importance.)

**Path**
  A path identifies a path within a GeoGit commit.
  A path uses similar notation to a file path on UNIX systems - a path is a sequence of string labels separated by "/". 
  A path can identify a layer or a feature within a GeoGit commit, or can point at subdivisions of a layer.

**Callback**
  For JSON output, GeoGit supports JSONP, a technique for making HTTP GET requests without violating the Same Origin Policy enforced by web browsers.
  JSONP is activated by providing a 'callback' parameter containing the name of JavaScript function to be invoked with the JSON returned by the GeoGit web API.
  Since callbacks are not applicable to XML output, when XML output is selected the callback parameter will be ignored.

**Output Format**
  The GeoGit web API supports both JSON and XML representations of all resources.
  While the recommended way of selecting the format is the standard HTTP header ``Accept``, the web API also accepts a form or query parameter named ``output_format``.
  This parameter may have the value "xml" to specify XML output (content-type ``application/xml``), or "json" to specify JSON (content-type ``application/json``).
  If neither the ``Accept`` header nor the ``output_format`` parameter specify a format, then JSON will be selected by default.

Endpoints
.........

``status``
  Generates a report of changes in the database which are unstaged or staged but uncommitted.
  Accepts query or form parameters:

  * ``offset`` - (Optional) A non-negative integer. Skip this many changes before reporting (for paging.)

  * ``limit`` - (Optional) A positive integer. Report no more than this number of results (for paging.)

  * the general ``output_format`` and ``callback`` parameters discussed above.

``log``
  Generates a report of changesets, starting with the newest changes and including progressively older commits.
  Accepts query or form parameters:

  * ``offset`` - (Optional) A non-negative integer.  Skip this many cmmits before reporting (for paging)

  * ``limit`` - (Optional) A positive integer.  Report no more than this number of results (for paging)

  * ``path`` - (Optional, may be repeated.) A path.
    If one or more paths are specified, only commits which modify descendants of one or more of the specified paths will be included in the results.

  * ``since`` - (Optional) A refspec identifying the earliest commit of the range of interest (no ancestors of this commit will be reported.)
  
  * ``until`` - (Optional) A refspec identifying the latest commit of the range of interest (no descendents of this commit will be reported.)

  * the general ``output_format`` and ``callback`` parameters discussed above.

``commit``
  Adds a new commit to history with changes from the working database.
  Accepts query or form parameters:

  * ``all`` - (Optional, defaults to false.) A boolean.  If it is true, any unstaged changes will be staged before committing.

  * ``message`` - (Required) A string, to be used as the commit message for the new commit.
    (Other metadata will be determined based on the credentials used for the request.)

  * the general ``output_format`` and ``callback`` parameters discussed above.

``ls-tree``
  List the contents of a tree from the GeoGit database.
  Trees are somewhat analogous to directories on a filesystem in that they may contain other trees or features, just as a filesystem directory may contain directories or normal files.
  Accepts query or form parameters:

  * ``showTree`` - (Optional, defaults to false.) A boolean.  If it is true then trees will be included when listing contents.
  
  * ``onlyTree`` - (Optional, defaults to false.)  If it is false then features will be included when listing contents.

  * ``recursive`` - (Optional, defaults to false.) A boolean.  If it is true then the report will include the contents of trees included in the report.

  * ``verbose`` - (Optional, defaults to false.) A boolean.  If it is false then the report will omit some detail to conserve bandwidth.

  * ``reflist`` - (Optional, may be repeated.) A refspec.  This specifies which tree or trees to report.

  * the general ``output_format`` and ``callback`` parameters discussed above.

``updateref``
  Modify the revision associated with a named ref (branch) on the server, or delete the named ref entirely
  Accepts query or form parameters:

  * ``name`` - (Required.) A string, naming the ref to be updated.

  * ``newValue`` - (Optional, mutually exclusive with ``delete``.)
    An object id to use as the new value.

  * ``delete`` - (Optional, mutually exclusive with ``newValue``.)
    A boolean, if true then the named ref will be removed from the server instead of having its value changed.

  * the general ``output_format`` and ``callback`` parameters discussed above.

  .. note:: Exactly one of ``newValue`` and ``delete`` must be provided.

``diff``
  Report the differences between two different points in history.
  Accepts query or form parameters:

  * ``oldRefSpec`` - (Required) A refspec identifying the "from" revision.
  
  * ``newRefSpec`` - (Required) A refspec identifying the "to" revision.

  * ``pathFilter`` - (Optional) A path within the repository.
    If specified, the diff will only include changes to features at or below this path.
    (Note that the changes may also modify the path of a feature!
    Both the original and final path of the feature are considered when applying this filter - one or both must match.)

  * the general ``output_format`` and ``callback`` parameters discussed above.

``ref-parse``
  Report the full object id for a partial object id, full object id, or named reference.

  * ``name`` - (Required) the partial object id, full object id, or named reference to resolve.

  * the general ``output_format`` and ``callback`` parameters discussed above.

Examples
========

Note: piping output into `json_pp` or `xmllint` will help.

JSON Status::

    curl -XPOST localhost:8182/status

Invalid Request::

    curl -XPOST localhost:8182/invalid

JSONP Status::

    curl -XPOST localhost:8182/status?callback=handle

XML Status (piped to xmllint for formatting)::

    curl -XPOST -H 'Accept: text/xml' localhost:8182/status | xmllint --format - 

The Future
==========

It would be trivial to expand the URL routing to one or more directory roots containing one
or more geogit repositories. For example:

  http://host/{directory}/{repo}/{command} 

To consider:

* authentication/authorization
* async processing if needed?
