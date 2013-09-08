# Elascast

A push engine to publish document stream to subscribers with high granular hierarchical filters

## Architecture

We have 3 tier architecture, mainly ElasticSearch cluster, redis pub/sub channels, and persistent connection socket server.

We use ElasticSearch cluster as document store. It can handle petabyte with sub 200ms latency with sufficient nodes.

For Aync push, we build persistent connection socket server using node.js to handle millions concurrent connections. Each client connection has a set of addressable tags that are used as query filters for selecting documents to push to clients. 

We use ElasticSearch Percolator to find out matching documents. When client connected, we register its addressable tags query to ES Percolator. Once matching documents are found, we publish them into redis server. Persistent connection socket server subscribes to various redis channels on behalf of connected clients and push matched documents to clients. 


## Configuration


## Usage

  lein run create-index
  lein run insert-doc 

## Demo




