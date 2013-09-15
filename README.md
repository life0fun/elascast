# Elascast

A push engine to publish document stream to subscribers with high granular hierarchical filters

## Architecture

We have 3 tier architecture, mainly ElasticSearch cluster, redis pub/sub channels, and persistent connection socket server that connects millions of users.

We use ElasticSearch cluster as document store. It can handle petabyte with sub 200ms latency with sufficient nodes.

For Aync push, we build persistent connection socket server using node.js to handle millions concurrent connections. Each client connection has a set of addressable tags that are used as query filters for selecting documents to push to clients. 

We use ElasticSearch Percolator to find out matching documents. When client connected, we register its addressable tags query to ES Percolator. Once matching documents are found, we publish them into redis server. Persistent connection socket server subscribes to various redis channels on behalf of connected clients and push matched documents to clients. 


## Percolate query

ES percolate queries are normal queries in which keyword is used to match against not_analyzed field. This is because document is submitted to percolate query before the doc is actually got indexed into ES. As a result, we need to match exactly the entire keyword, or we use wildcard queries, or use Fuzzy and Fuzzy like queries.

Document percolation returns a map with :matches key contains a list of matching query-name.

We should encode client information into query name so we can directly know which client we should push the doc directly to after percolation match.

## Example

When user registers queries to percolator, we use wildcard query to match against the address field by adding wildcard to the begining and the end of the keyword.
This is very coarse-grained solution. More refined solution will be using regexp query, or Fuzzy query and Fuzzy like query to filter the address field.


## Configuration


## Usage
  
First, create mapping
  lein run create-index

Second, register queries
  lein run register query-name keyword

Finally, submit doc to percolate
  lein run submit-doc ./data/events.txt


## Demo

We have example docs under data/events.txt. Each doc consists of 3 lines, the first is the author of doc, the second is doc text, and the 3rd line is the addresses that the author want the document to be pushed to.

When user registers query with keyword "coder", both docs matches under  east:coder:lucene and pacific:coder:elastic addresses.

when registering with "pacific:coder", only the document with pacific:coder:elastic address matches.

This showes how we controll the addressing in a great flexibility as well as in a highly fine-granularity level.


