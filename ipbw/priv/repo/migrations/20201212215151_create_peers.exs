defmodule Ipbw.Repo.Migrations.CreatePeers do
  use Ecto.Migration

  def change do
    create table(:peers) do
      add :pid, :string
      add :ip, :string

      timestamps()
    end

  end
end
