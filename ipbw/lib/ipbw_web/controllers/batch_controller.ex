defmodule IpbwWeb.BatchController do
  use IpbwWeb, :controller

  alias Ipbw.Peers
  alias Ipbw.Peers.Peer

  action_fallback IpbwWeb.FallbackController

  @doc """
  Submit a batch of new peers to insert:

  {
    "peers": [
      {
        "pid": "UCgysaduic...",
        "ip": "0.0.0.0",
        "timestamp": "<timestamp utc>",
        "neighbours": ["p1", "p2", "p3"]
      }
    ]
  }
  """
  def submit(conn, %{"peers" => peer_list}) do
    peer_list |> Enum.map(&Peers.create_peer/1)
    require Logger
    Logger.info("Got peer batch", peer_list)
    conn
  end

end
