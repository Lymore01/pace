defmodule EngineWeb.Redis.RedisClient do
  use GenServer
  require Logger

  def start_link(port \\ 6379) do
    GenServer.start_link(__MODULE__, port, name: __MODULE__)
  end

  def command(command) do
    GenServer.call(__MODULE__, {:send, command})
  end

  # callbacks
  @impl true
  def init(port) do
    case :gen_tcp.connect(~c"localhost", port, [:binary, packet: :raw, active: false]) do
      {:ok, socket} ->
        Logger.info("Connected to Redis on port #{port}")
        {:ok, %{socket: socket}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:send, command_string}, _from, %{socket: socket} = state) do
    encoded = encode(command_string)

    :ok = :gen_tcp.send(socket, encoded)

    case :gen_tcp.recv(socket, 0) do
      {:ok, raw_data} ->
        {:reply, decode(raw_data), state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # protocol Logic

  defp encode(command) when is_binary(command) do
    parts = String.split(command)
    header = "*#{length(parts)}\r\n"

    body =
      Enum.map_join(parts, fn part ->
        "$#{byte_size(part)}\r\n#{part}\r\n"
      end)

    header <> body
  end

  defp encode(command) when is_list(command) do
    header = "*#{length(command)}\r\n"

    body =
      Enum.map_join(command, fn part ->
        part_str = to_string(part)
        "$#{byte_size(part_str)}\r\n#{part_str}\r\n"
      end)

    header <> body
  end


  defp decode(<<"+", rest::binary>>), do: String.trim_trailing(rest, "\r\n")

  defp decode(<<"-", rest::binary>>), do: {:error, String.trim_trailing(rest, "\r\n")}

  defp decode(<<":", rest::binary>>) do
    rest |> String.trim_trailing("\r\n") |> String.to_integer()
  end

  defp decode(<<"$", rest::binary>>) do
    case String.split(rest, "\r\n", parts: 2) do
      ["-1", _] ->
        nil

      [len_str, data] ->
        len = String.to_integer(len_str)
        binary_part(data, 0, len)
    end
  end

  defp decode(<<"*", rest::binary>>) do
    case String.split(rest, "\r\n", parts: 2) do
      # Null Array
      ["-1", _] ->
        nil

      # Empty Array
      ["0", _] ->
        []

      [len_str, elements_data] ->
        count = String.to_integer(len_str)
        {items, _remaining_buffer} = decode_multi(elements_data, count, [])
        items
    end
  end

  defp decode_multi(buffer, 0, acc), do: {Enum.reverse(acc), buffer}

  defp decode_multi(buffer, count, acc) do
    {item, rest} = decode_with_rest(buffer)
    decode_multi(rest, count - 1, [item | acc])
  end

  defp decode_with_rest(<<"+", rest::binary>>) do
    [val, remaining] = String.split(rest, "\r\n", parts: 2)
    {val, remaining}
  end

  defp decode_with_rest(<<":", rest::binary>>) do
    [val, remaining] = String.split(rest, "\r\n", parts: 2)
    {String.to_integer(val), remaining}
  end

  defp decode_with_rest(<<"$", rest::binary>>) do
    [len_str, data] = String.split(rest, "\r\n", parts: 2)
    len = String.to_integer(len_str)

    val = binary_part(data, 0, len)
    remaining = binary_part(data, len + 2, byte_size(data) - len - 2)

    {val, remaining}
  end

end
