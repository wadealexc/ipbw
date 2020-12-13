defmodule IpbwWeb.PageController do
  use IpbwWeb, :controller

  def index(conn, _params) do
    render(conn, "index.html")
  end
end
