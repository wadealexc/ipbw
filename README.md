# ipbw

Interplanetary Black Widow - crawls ur IPFS nodes

## About

Better README coming soon to an IKEA near you.

## Usage

Just `go run main.go`. Configurability is a trap and should be avoided.

## Output

Last version's output is below. This version can swing some 5000 unique peers discovered in under a minute 😎

The output of a 10-minute crawl was rendered in Cytoscape using an edge-weighted, force-based layout algorithm. Some discovery statistics:
* 600 unique peers discovered within 1 minute
* 2450 unique peers discovered within 5 minutes
* 3600 unique peers discovered within 10 minutes

![Image](graph.png)
