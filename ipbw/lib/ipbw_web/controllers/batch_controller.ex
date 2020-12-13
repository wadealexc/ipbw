defmodule IpbwWeb.BatchController do
  use IpbwWeb, :controller

  alias Ipbw.Peers
  alias Ipbw.Peers.Peer

  action_fallback IpbwWeb.FallbackController

  @doc """
  SUbmit a batch of new peers to insert
  """
  def submit(conn, params) do
    
  end

end
