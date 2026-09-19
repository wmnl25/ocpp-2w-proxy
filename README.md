# ocpp-2w-proxy

A 2 way OCPP proxy (can also be used as a 1-way simple proxy).

ocpp-2w-proxy allows chargers (one or more) to establish connections with one or more central management systems (servers).
This is useful for example if a setup requires that the charger wants to connect to an official server (e.g. for billing purposes), 
and at the same time the happy EV person would like to also view and control the charger from e.g. Home Assistant.

The proxy works by defining a list of servers. The first server is primary; any remaining servers are secondary. This governs the way in which the proxy will forward messages upstream and downstream.

The rules are as follows:
1. All Calls (OCPP type 2) from the charger are forwarded to every configured server.
2. All Replies (OCPP type 3) or Errors (OCPP type 4) received from the primary server are forwarded to the charger.
3. All Replies (OCPP type 3) or Errors (OCPP type 4) received from secondary servers are ignored and not forwarded to the charger.
4. All Calls (OCPP type 2) received from any server are forwarded to the charger. The message ID is tracked against the server.
5. All Replies (OCPP type 3) or Errors (OCPP type 4) received from the charger are forwarded to the server that sent the corresponding Call.

The proxy will also keep a watch dog of stale connections. If a connection is not seen for more than `watchdog_stale` seconds, the connection will be closed and removed from the list.

Raw websocket payload logging is optional and disabled by default. To enable it, set
`websocket_data_file` under `[logging]`, for example:

`websocket_data_file = websocket-data.log`

The file contains every payload received from and sent to the charger and upstream
servers. Payloads may contain sensitive data, so protect the log file appropriately.


## Usage

Review and update configuration in `ocpp-2w-proxy.ini`. Configure multiple upstream servers with a comma-separated `servers` value; the first URL is primary:

`servers = ws://primary.example, ws://secondary.example, wss://monitor.example`

The legacy `server` and optional `secondary_server` settings remain supported. Start the proxy with:

`python ocpp_2w_proxy.py`

## Docker

`Dockerfile` and `compose.yaml` files are included for completeness.

 