# thi.ng/ldk-core

Lightweight Linked Data tools for Clojure & Clojurescript.

LDK provides an extensible architecture for working with Linked Data
(using the W3C RDF model) and so far includes:

- RDF value coercions & RDF container/collection type creation
- Named graphs
- database agnostic storage layer
- query engine with SPARQL like features & result serializations as
  CSV & JSON-LD
- Turtle I/O, CSV to RDF conversion utilities
- Simple rulebased inference engine
- graph export as Graphviz
- mapping of RDF graphs as object trees (nested Clojure maps)

## Description & usage

See [index.org](src/index.org)

## License

Copyright © 2013-2014 Karsten Schmidt

Distributed under the [Apache Software License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
