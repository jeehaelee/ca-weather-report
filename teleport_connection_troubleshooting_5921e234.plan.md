---
name: Teleport connection troubleshooting
overview: The timeout errors mean your machine cannot reach DoorDash’s internal Teleport proxy. Fixing it almost always means being on VPN and checking network/DNS; the guide’s login steps are correct.
todos: []
isProject: false
---

# Teleport connection troubleshooting

## What the errors mean

- `**Client.Timeout exceeded while awaiting headers**` and `**context deadline exceeded**` mean the `tsh` client never got a response from `https://proxy.doordash-int.com:443`. The request is timing out before a connection is established.

So the problem is **reachability** of the proxy, not your Okta credentials or Teleport permissions.

## Most likely cause: VPN

`proxy.doordash-int.com` is an internal host. It is usually only reachable when you are:

- On DoorDash corporate network, or  
- Connected to **DoorDash VPN**.

**Action:** Connect to DoorDash VPN, then run again:

```bash
tsh --proxy=proxy.doordash-int.com:443 --auth=okta login
```

## If you’re already on VPN

1. **Confirm VPN is up**
  Check your VPN client; ensure you’re connected and that it’s the correct DoorDash/corporate VPN profile.
2. **Test basic reachability**
  In a terminal:

```bash
   ping -c 3 proxy.doordash-int.com
   

```

   If this fails or times out, the proxy is not reachable from your current network (VPN/network issue, not Teleport config).

1. **Check DNS**
  Ensure `proxy.doordash-int.com` resolves to an internal IP when on VPN:

```bash
   nslookup proxy.doordash-int.com
   

```

   If it doesn’t resolve or resolves to something unexpected, VPN or DNS (split-tunnel/DNS over VPN) may be misconfigured.

1. **Try from a different network**
  If you’re on a restricted Wi‑Fi (e.g. guest, café), try from home or another network while on VPN to rule out local firewall blocking outbound 443.

## Summary


| Symptom                                     | Likely cause                            | What to do                                                                                                                                                                                              |
| ------------------------------------------- | --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Timeout / deadline exceeded                 | Proxy not reachable                     | Connect to DoorDash VPN, retry `tsh ... login`                                                                                                                                                          |
| Still times out on VPN                      | VPN or network/DNS issue                | Check VPN status, `ping` and `nslookup` to proxy host                                                                                                                                                   |
| “You do not have access” (after connecting) | Browser/default browser or device trust | Per [Backend Dev Environment Setup Guide](file:///Users/jeehae.lee/Downloads/Backend%20Dev%20Environment%20Setup%20Guide.pdf): set Chrome as default browser; if it persists, contact **#device-trust** |


Your `tsh` command matches the guide; no change to the command is needed. Resolving the timeout is almost certainly a matter of being on VPN and ensuring the proxy is reachable from your machine.