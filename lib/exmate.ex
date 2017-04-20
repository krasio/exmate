defmodule Exmate do
  @moduledoc """
  Documentation for Exmate.
  """

  @doc """
  Hello world.

  ## Examples

      iex> Exmate.hello
      :world

  """
  def hello do
    :world
  end

  defmodule Item do
    @derive [Poison.Encoder]
    defstruct [:term, :kind, :raw, :id, :rank]
  end

  def bulk_load(stream, kind, conn) do
    total = stream
            |> Stream.map(&parse_item(&1, kind))
            |> Stream.filter(&(&1 != false))
            |> Stream.map(&load_item(&1, conn))
            |> Enum.to_list
            |> Enum.count

    {:ok, total}
  end

  def query(q, kind, conn) do
    words = q |> normalize |> String.split(" ") |> Enum.filter(fn(w) -> String.length(w) > 2 end)
    {:ok, query_words(Enum.sort(words), kind, conn)}
  end

  def cleanup(kind, conn) do
    Redix.command!(conn, ["SMEMBERS", base(kind)])
    |> Enum.each(fn(p) -> 
      Redix.command!(conn, ["DEL", "#{base(kind)}:#{p}", "#{cachebase(kind)}:#{p}"])
    end)

    Redix.command!(conn, ["DEL", base(kind), database(kind), cachebase(kind)])
  end

  defp query_words([], _, _), do: []

  defp query_words(words, kind, conn) do
    cachekey = cachebase(kind) <> ":" <> Enum.join(words, "|")
    exists = Redix.command!(conn, ["EXISTS", cachekey])
    if exists == 0 do
      interkeys = Enum.map(words, fn(w) -> "#{base(kind)}:#{w}" end)
      Redix.command!(conn, ["ZINTERSTORE", cachekey, Enum.count(interkeys)] ++ interkeys)
      Redix.command!(conn, ["EXPIRE", cachekey, 10*60])
    end

    ids = Redix.command!(conn, ["ZREVRANGE", cachekey, 0, 5-1])
    Redix.command!(conn, ["HMGET", database(kind)] ++ ids)
    |> Enum.map(fn(raw) ->
      item = Poison.decode!(raw, as: %Item{})
      %{item | kind: kind, raw: String.trim(raw)}
    end)
  end

  defp parse_item(raw, kind) do
    with {:ok, item} <- Poison.decode(raw, as: %Item{})
    do
      %{item | kind: kind, raw: String.trim(raw)}
    else
      _ -> false
    end
  end

  defp load_item(item, conn) do
    Redix.command!(conn, ["HSET", database(item.kind), item.id, item.raw])
    Enum.each(prefixes_for_phrase(item.term), fn(prefix) ->
      Redix.command!(conn, ["SADD", base(item.kind), prefix])
      Redix.command!(conn, ["ZADD", "#{base(item.kind)}:#{prefix}", item.rank, item.id])
    end)

    item
  end

  defp database(kind) do
    "exmate-data:" <> kind
  end

  defp base(kind) do
    "exmate-index:" <> kind
  end

  defp cachebase(kind) do
    "exmate-cache:" <> kind
  end

  def prefixes_for_phrase(phrase) do
    phrase
    |> normalize
    |> String.split(" ")
    |> Enum.flat_map(fn(word) -> prefixes_for(word) end)
  end

  defp prefixes_for(word) do
    prefixes_for(word, 2, [])
  end

  defp prefixes_for(word, index, results) do
    if index == String.length(word) do
      results
    else
      prefixes_for(word, index + 1, results ++ [String.slice(word, 0..index)])
    end
  end

  defp normalize(phrase) do
    phrase
    |> String.trim
    |> String.downcase
    |> String.replace( ~r/[^\w ]/ui, "")
  end
end
