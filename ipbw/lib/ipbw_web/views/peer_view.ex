defmodule IpbwWeb.PeerView do
  use IpbwWeb, :view
  alias IpbwWeb.PeerView

  def render("index.json", %{peers: peers}) do
    %{data: render_many(peers.entries, PeerView, "peer.json"),
      pagination: %{
        page_number: peers.page_number,
        total_pages: peers.total_pages,
        page_size: peers.page_size,
        total_entries: peers.total_entries
      }}
  end

  def render("show.json", %{peer: peer}) do
    %{data: render_one(peer, PeerView, "peer.json")}
  end

  def render("peer.json", %{peer: peer}) do
    %{id: peer.id,
      pid: peer.pid,
      ip: peer.ip}
  end
end
