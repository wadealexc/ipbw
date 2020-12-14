defmodule Ipbw.Repo.Migrations.CreateConnections do
  use Ecto.Migration

  def change do
    create table(:connections, primary_key: false) do
      add :id, :uuid, primary_key: true

      add :origin, references(:peers, on_delete: :nothing)
      add :destination, references(:peers, on_delete: :nothing)

      timestamps()
    end

    create index(:connections, [:origin])
    create index(:connections, [:destination])
  end
end
