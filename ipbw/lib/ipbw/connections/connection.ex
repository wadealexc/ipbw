defmodule Ipbw.Connections.Connection do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}

  schema "connections" do
    field :origin, :id
    field :destination, :id

    timestamps()
  end

  @doc false
  def changeset(connection, attrs) do
    connection
    |> cast(attrs, [])
    |> validate_required([])
  end
end
