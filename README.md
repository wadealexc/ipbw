# ipbw
IPBW: Interplanetary Black Widow crawls ur ipfs nodes

## About

IPBW uses `go-libp2p` and `go-libp2p-kad-dht` to crawl the IPFS network. IPBW spawns workers to generate random peer IDs and execute DHT queries to connected peers. The DHT uses an XOR distance calculation to select up to K connected peers to query for each random peer ID. Peers respond with their own connected peers within a certain distance of the requested random peer ID.

Over time, recursive queries to discovered peers enumerate the network.

## Usage

`./ipbw -d=<duration>`

* [Optional] Use -d to specify the number of minutes to crawl for
    * By default, the crawler runs until it receives an interrupt

IPBW creates 2 files on termination:
* `peerIDs.csv`: stores IDs of discovered peers, along with the ID of the peer who responded to our query. Records take the form: "pOrigin,pResponse" ... where each field is a peer ID
* `peerIPs.csv`: stores IPv4/IPv6/DNS4/DNS6 addresses of peers, along with the ID of the peer located at that address. Records take the form: "pID,ip:port"

## Output

The output of a 10-minute crawl was rendered in Cytoscape using an edge-weighted, force-based layout algorithm. Some discovery statistics:
* 600 unique peers discovered within 1 minute
* 2450 unique peers discovered within 5 minutes
* 3600 unique peers discovered within 10 minutes

![Image](graph.png)
