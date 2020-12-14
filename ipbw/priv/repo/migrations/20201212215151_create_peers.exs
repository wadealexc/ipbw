defmodule Ipbw.Repo.Migrations.CreatePeers do
  use Ecto.Migration

  def change do
    create table(:peers, primary_key: false) do
      add :id, :uuid, primary_key: true

      add :peer_id, :string
      add :ip, :string

      timestamps()
    end

    create index(:[:peers], [:peer_id])
  end
end
