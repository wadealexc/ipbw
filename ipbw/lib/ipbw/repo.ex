defmodule Ipbw.Repo do
  use Ecto.Repo,
    otp_app: :ipbw,
    adapter: Ecto.Adapters.Postgres
  use Scrivener, page_size: 10
end
