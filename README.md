# ipbw

Interplanetary Black Widow - crawls ur IPFS nodes

## About

IPBW uses `go-libp2p` and `go-libp2p-kad-dht` to crawl the IPFS network. IPBW spawns workers to generate random peer IDs and execute DHT queries to connected peers. The DHT uses an XOR distance calculation to select up to K connected peers to query for each random peer ID. Peers respond with their own connected peers within a certain distance of the requested random peer ID. Over time, recursive queries to discovered peers enumerate the network.

## Usage

```
NAME:
   Interplanetary Black Widow - crawls ur IPFS nodes

USAGE:
   ipbw [global options] command [command options] [arguments...]

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --crawl-duration value, -d value     specify the number of minutes the crawler should run for (default: 5)
   --num-workers value, -n value        specify the number of goroutines used to query the DHT (default: 8)
   --enable-status, -s                  specify whether the crawler should output occasional status updates to console. (default: true)
   --status-interval value, --si value  how often (in minutes) status updates will be posted (default: 1)
   --enable-identifier, -i              enables the identifier module (default: false)
   --enable-reporter, -r                enables the reporter module, which publishes results to a server (default: false)
   --report-interval value, --ri value  how often (in minutes) the reporter will publish crawl results to the server (default: 1)
   --report-endpoint value, --re value  the url/endpoint the reporter will publish crawl results to (default: "http://127.0.0.1:8000/batch")
   --report-ping value, --rp value      the url/endpoint the reporter will ping on startup to ensure the server is running (default: "http://127.0.0.1:8000/healthcheck")
   --report-api-key value, -a value   the API key used to authenticate the reporter's reports
   --help, -h                           show help (default: false)
```

## Output

The output of a 10-minute crawl was rendered in Cytoscape using an edge-weighted, force-based layout algorithm. Some discovery statistics:
* 600 unique peers discovered within 1 minute
* 2450 unique peers discovered within 5 minutes
* 3600 unique peers discovered within 10 minutes

![Image](output/graph.png)
