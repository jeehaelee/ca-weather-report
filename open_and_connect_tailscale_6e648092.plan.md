---
name: Open and connect Tailscale
overview: Get the Tailscale app to open on macOS and sign in so you can connect to the preferred VPN network for DoorDash (proxy/Teleport).
todos: []
isProject: false
---

# Open and connect to Tailscale

## 1. Open the Tailscale app

Tailscale on macOS runs as a **menu bar app**. After launch it may show only in the menu bar (top right), not as a regular window.

**Ways to open it:**

- **Spotlight:** Press `Cmd + Space`, type `Tailscale`, press Enter.
- **Finder:** Open **Applications**, find **Tailscale** (or "Tailscale VPN"), double-click.
- **Terminal:** Run:

```bash
  open -a Tailscale
  


```

If it’s already running, look for the **Tailscale icon** (overlapping circles) in the **menu bar**. Click it to see status and options.

---

## 2. If the app doesn’t open or crashes

- **macOS permissions:** Go to **System Settings → Privacy & Security**. Check **Accessibility** and **Full Disk Access** (and any VPN/network-related entries) and ensure Tailscale is allowed if it appears there.
- **Re-launch:** In Terminal:

```bash
  killall Tailscale 2>/dev/null; open -a Tailscale
  

```

- **Reinstall:** Remove via the software manager (the "REMOVE" button you saw), then install again from the same place or from [tailscale.com/download](https://tailscale.com/download). Restart the Mac if the app previously crashed on launch.

---

## 3. Connect to the Tailscale network

Once the app is open (menu bar icon visible):

1. Click the **Tailscale icon** in the menu bar.
2. Choose **Log in** (or **Sign in**).
3. Sign in with the identity your company uses (e.g. **Google** or **Microsoft** with your **@doordash.com** account). DoorDash may use a custom IdP; use the option IT specified.
4. If your org uses **Tailscale ACLs**, an admin may need to **approve your device** before you get access. Check internal docs or ask in **#device-trust** / IT for “Tailscale” or “VPN.”

**Note:** Tailscale and “AWS VPN” are different. For Teleport (`proxy.doordash-int.com`), you need to be on the network that can reach that proxy—often Tailscale at DoorDash. Once connected to Tailscale, retry:

```bash
tsh --proxy=proxy.doordash-int.com:443 --auth=okta login
```

---

## 4. If you still can’t open Tailscale

- Check **Activity Monitor** for “Tailscale” and force-quit it, then try opening again.
- Look in **Console** (Applications → Utilities → Console) for “Tailscale” crash logs and share the error with IT or #device-trust.

If your org manages devices with Munki/Jamf, Tailscale might be deployed but require a **login or post-install step** from internal docs; those channels can confirm the exact steps for DoorDash.