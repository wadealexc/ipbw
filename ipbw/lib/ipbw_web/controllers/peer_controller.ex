defmodule IpbwWeb.PeerController do
  use IpbwWeb, :controller

  alias Ipbw.Peers
  alias Ipbw.Peers.Peer

  action_fallback IpbwWeb.FallbackController

  def index(conn, params) do
    peers = Peers.list_peers(params)
    render(conn, "index.json", peers: peers)
  end

  def create(conn, %{"peer" => peer_params}) do
    with {:ok, %Peer{} = peer} <- Peers.create_peer(peer_params) do
      conn
      |> put_status(:created)
      |> put_resp_header("location", Routes.peer_path(conn, :show, peer))
      |> render("show.json", peer: peer)
    end
  end

  def show(conn, %{"id" => id}) do
    peer = Peers.get_peer!(id)
    render(conn, "show.json", peer: peer)
  end

  def update(conn, %{"id" => id, "peer" => peer_params}) do
    peer = Peers.get_peer!(id)

    with {:ok, %Peer{} = peer} <- Peers.update_peer(peer, peer_params) do
      render(conn, "show.json", peer: peer)
    end
  end

  def delete(conn, %{"id" => id}) do
    peer = Peers.get_peer!(id)

    with {:ok, %Peer{}} <- Peers.delete_peer(peer) do
      send_resp(conn, :no_content, "")
    end
  end
end
