defmodule Ipbw.PeersTest do
  use Ipbw.DataCase

  alias Ipbw.Peers

  describe "peers" do
    alias Ipbw.Peers.Peer

    @valid_attrs %{ip: "some ip", pid: "some pid"}
    @update_attrs %{ip: "some updated ip", pid: "some updated pid"}
    @invalid_attrs %{ip: nil, pid: nil}

    def peer_fixture(attrs \\ %{}) do
      {:ok, peer} =
        attrs
        |> Enum.into(@valid_attrs)
        |> Peers.create_peer()

      peer
    end

    test "list_peers/0 returns all peers" do
      peer = peer_fixture()
      assert Peers.list_peers() == [peer]
    end

    test "get_peer!/1 returns the peer with given id" do
      peer = peer_fixture()
      assert Peers.get_peer!(peer.id) == peer
    end

    test "create_peer/1 with valid data creates a peer" do
      assert {:ok, %Peer{} = peer} = Peers.create_peer(@valid_attrs)
      assert peer.ip == "some ip"
      assert peer.pid == "some pid"
    end

    test "create_peer/1 with invalid data returns error changeset" do
      assert {:error, %Ecto.Changeset{}} = Peers.create_peer(@invalid_attrs)
    end

    test "update_peer/2 with valid data updates the peer" do
      peer = peer_fixture()
      assert {:ok, %Peer{} = peer} = Peers.update_peer(peer, @update_attrs)
      assert peer.ip == "some updated ip"
      assert peer.pid == "some updated pid"
    end

    test "update_peer/2 with invalid data returns error changeset" do
      peer = peer_fixture()
      assert {:error, %Ecto.Changeset{}} = Peers.update_peer(peer, @invalid_attrs)
      assert peer == Peers.get_peer!(peer.id)
    end

    test "delete_peer/1 deletes the peer" do
      peer = peer_fixture()
      assert {:ok, %Peer{}} = Peers.delete_peer(peer)
      assert_raise Ecto.NoResultsError, fn -> Peers.get_peer!(peer.id) end
    end

    test "change_peer/1 returns a peer changeset" do
      peer = peer_fixture()
      assert %Ecto.Changeset{} = Peers.change_peer(peer)
    end
  end
end
