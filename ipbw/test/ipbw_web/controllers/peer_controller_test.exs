defmodule IpbwWeb.PeerControllerTest do
  use IpbwWeb.ConnCase

  alias Ipbw.Peers
  alias Ipbw.Peers.Peer

  @create_attrs %{
    ip: "some ip",
    pid: "some pid"
  }
  @update_attrs %{
    ip: "some updated ip",
    pid: "some updated pid"
  }
  @invalid_attrs %{ip: nil, pid: nil}

  def fixture(:peer) do
    {:ok, peer} = Peers.create_peer(@create_attrs)
    peer
  end

  setup %{conn: conn} do
    {:ok, conn: put_req_header(conn, "accept", "application/json")}
  end

  describe "index" do
    test "lists all peers", %{conn: conn} do
      conn = get(conn, Routes.peer_path(conn, :index))
      assert json_response(conn, 200)["data"] == []
    end
  end

  describe "create peer" do
    test "renders peer when data is valid", %{conn: conn} do
      conn = post(conn, Routes.peer_path(conn, :create), peer: @create_attrs)
      assert %{"id" => id} = json_response(conn, 201)["data"]

      conn = get(conn, Routes.peer_path(conn, :show, id))

      assert %{
               "id" => id,
               "ip" => "some ip",
               "pid" => "some pid"
             } = json_response(conn, 200)["data"]
    end

    test "renders errors when data is invalid", %{conn: conn} do
      conn = post(conn, Routes.peer_path(conn, :create), peer: @invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end
  end

  describe "update peer" do
    setup [:create_peer]

    test "renders peer when data is valid", %{conn: conn, peer: %Peer{id: id} = peer} do
      conn = put(conn, Routes.peer_path(conn, :update, peer), peer: @update_attrs)
      assert %{"id" => ^id} = json_response(conn, 200)["data"]

      conn = get(conn, Routes.peer_path(conn, :show, id))

      assert %{
               "id" => id,
               "ip" => "some updated ip",
               "pid" => "some updated pid"
             } = json_response(conn, 200)["data"]
    end

    test "renders errors when data is invalid", %{conn: conn, peer: peer} do
      conn = put(conn, Routes.peer_path(conn, :update, peer), peer: @invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end
  end

  describe "delete peer" do
    setup [:create_peer]

    test "deletes chosen peer", %{conn: conn, peer: peer} do
      conn = delete(conn, Routes.peer_path(conn, :delete, peer))
      assert response(conn, 204)

      assert_error_sent 404, fn ->
        get(conn, Routes.peer_path(conn, :show, peer))
      end
    end
  end

  defp create_peer(_) do
    peer = fixture(:peer)
    %{peer: peer}
  end
end
