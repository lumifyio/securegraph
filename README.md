securegraph
-----------

Secure graph is an API to manipulate graphs, similar to blueprints. Unlike
blueprints, every Secure graph method require authorizations and visibilities.
Secure graph also supports multivalued properties as well as property metadata.

Accumulo Iterators
------------------
The Accumulo implementation of secure graph can make use of server-side iterators to improve performance by limiting rows returned by tablet servers to only those where the end user has the proper authorizations. This requires copying the `securegraph-accumulo-iterators-*.jar` file to `$ACCUMULO_HOME/lib/ext` on each Accumulo server. Use `mvn package` to build the required JAR file.

License
-------

Copyright 2013 Altamira Technologies Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
