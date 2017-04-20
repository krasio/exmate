defmodule ExmateTest do
  use ExUnit.Case
  doctest Exmate

  test "the load data and query" do
    # Flush Redis DB
    {:ok, conn} = Redix.start_link("redis://localhost:9998/5", name: :redix)
    Redix.command(conn, ~w(FLUSHDB))

    # Load data
    support_dir =  Path.join(~w(#{File.cwd!} test support))
    data_stream = File.stream!("#{support_dir}/suburbs.json")

    {:ok, items_loaded} = Exmate.bulk_load(data_stream, "suburb", conn)
    assert 3 == items_loaded, "Expected to get 3 items loaded but got #{items_loaded}."

    # Query
    {:ok, results} = Exmate.query("we", "suburb", conn)
    assert 0 == Enum.count(results), ~s(Expected to get no matches for "we" but got #{Enum.count(results)}.)

    {:ok, results} = Exmate.query("wel", "suburb", conn)
    assert 2 == Enum.count(results), ~s(Expected to get 2 matches for "wel" but got #{Enum.count(results)}.)

    term = Enum.at(results, 0).term
    assert "Wellington" == term, ~s(Expected get Wellington as top match for "wel" but got #{term})

    term = Enum.at(results, 1).term
    assert "Welly" == term, ~s(Expected get Welly as top match for "wel" but got #{term})

    # Cleanup
    Exmate.cleanup("suburb", conn)
    keys = Redix.command!(conn, ["KEYS", "exmate-*"])
    assert 0 == Enum.count(keys), ~s(Expect 0 exmate keys after cleanup, got #{Enum.count(keys)})
  end
end
