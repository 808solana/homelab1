# N100 Mini-PC — Live Context Handoff

> Purpose: give a fresh Claude (different desktop / sandbox) accurate, current
> context about the Debian N100 mini-PC before we install new Docker containers,
> local AI tooling, and OpenRouter-based services on it.
>
> Source: live read-only SSH probe of the box.
> Probe date: **2026-06-15** | Probe time: **12:15 PM MST (Phoenix, Arizona)**
> Nothing on the server was modified. No secrets are included in this file.

---

## 1. How to reach the box

**Two addresses, two jobs — not interchangeable:**

- **Tailscale `100.90.62.96`** — the remote-access overlay. This is how the
  Windows workstation (`bld`) and **Cursor reach the box from anywhere**. Use it
  for **SSH** and for the **NPM admin UI** (`:81`) when you're off-LAN. Works
  whether you're at home or remote.
- **LAN static `192.168.0.150`** — the box's address on its own local network
  (Wi-Fi `wlo1`; wired `enp1s0` is down). **NPM runs on the box and forwards to
  services using this IP** — every existing Proxy Host points at
  `192.168.0.150:<port>`. Reachable only on the local network.

Rule of thumb: **SSH / admin from your desktop → Tailscale.** **NPM → a service on
the box → `192.168.0.150:<host-port>`.** Don't put the LAN IP in the SSH target
(not routable when remote), and don't point NPM at the Tailscale IP.

- **SSH target:** `kor@100.90.62.96` (Tailscale), **key auth only**. Key path lives
  in the gitignored `.env` as `DEBIAN_SSH_KEY_PATH` (user in `DEBIAN_SSH_USER`,
  host in `DEBIAN_SSH_HOST`). Do **not** hardcode or print the key.

Example (key path read from `.env`, not shown here):

```bash
ssh -i <DEBIAN_SSH_KEY_PATH> kor@100.90.62.96
```

---

## 2. Identity & network

| Item | Value |
|---|---|
| SSH user (`whoami`) | `kor` |
| Hostname | `kor` |
| Tailscale IP (this host) | `100.90.62.96` (machine name `kor`, linux) — remote access (SSH/admin) |
| LAN static IP | `192.168.0.150` (Wi-Fi `wlo1`) — NPM forwards to services here |
| Tailnet owner tag | `resellin101@` |

**Tailnet peers seen:**

- `kor` — `100.90.62.96` — linux — **this server**
- `bld` — `100.126.37.71` — windows — **active, direct connection** (Windows
  workstation / Cursor host talking to the box).

---

## 3. OS & hardware

| Item | Value |
|---|---|
| OS | Debian GNU/Linux 13 (trixie), version 13 |
| Kernel | `6.12.88+deb13-amd64` (x86-64) |
| CPU | Intel **N100**, **4 cores** (`nproc` = 4) |
| GPU | **None / iGPU only** (no NVIDIA/CUDA) |
| RAM | **15 GiB total** (~16 GB). At probe: 1.8 GiB used, ~13 GiB available |
| Hardware | Trigkey "Key Mini", ~900 GB NVMe + 512 GB SSD |
| Uptime at probe | **10 days, 4 hours** |

**Implication:** no discrete GPU + 16 GB RAM means **heavy local LLM inference is
not practical**. Use **OpenRouter (cloud) APIs** for model calls. Run lightweight
glue/UI containers locally (e.g. LiteLLM, Open WebUI, n8n) pointing at OpenRouter.

---

## 4. Storage

### Disk layout (`lsblk -f`)

| Device | FS | Label/UUID (short) | Avail | Use% | Mountpoint |
|---|---|---|---|---|---|
| `sda1` | ext4 | `5849cf82…` | 444.5 G | 0% | `/mnt/storage` |
| `nvme0n1p1` | vfat FAT32 | `8A30-DBE9` | 965.3 M | 1% | `/boot/efi` |
| `nvme0n1p2` | ext4 | `0bf2f170…` | 17.6 G | **63%** | `/` |
| `nvme0n1p3` | swap | `efa46821…` | — | — | `[SWAP]` |
| `nvme0n1p4` | ext4 | `6da9e807…` | 800.5 G | 0% | `/home` |

### Filesystem mounts (`df -h`)

| Mount | Device | Size | Used | Avail | Use% |
|---|---|---|---|---|---|
| `/` | `/dev/nvme0n1p2` | 56 G | 36 G | 18 G | **67%** |
| `/boot/efi` | `/dev/nvme0n1p1` | 975 M | 8.8 M | 966 M | 1% |
| `/home` | `/dev/nvme0n1p4` | 844 G | 2.2 M | **801 G** | 0% |
| `/mnt/storage` | `/dev/sda1` | 469 G | 2.1 M | **445 G** | 1% |
| `tmpfs` (`/run`) | tmpfs | 1.6 G | 1.9 M | 1.6 G | 1% |
| `tmpfs` (`/tmp`) | tmpfs | 7.7 G | 0 | 7.7 G | 0% |
| Swap | nvme0n1p3 | 15 Gi | 0 | 15 Gi | 0% |

**Storage summary:**
- **`/mnt/storage`** (sda1, ext4, 469 GB) — essentially empty (only `lost+found`).
  Owned by `kor:kor`. Ideal for container bind-mount data and backups.
- **`/home`** (nvme0n1p4, 844 GB) — ~801 GB free. Secondary large-capacity target.
- **`/`** (nvme0n1p2, 56 GB) — 36 GB used / 18 GB free (67%). Watch this — keep
  app data off the OS partition.
- Mounted with `nofail` for `/mnt/storage` (boot-safe). fstab backup at `/etc/fstab.bak`.

> **Love project lives at `/mnt/storage/love`**; keep its data (e.g. SQLite later)
> under `/mnt/storage/love/data`.

---

## 5. Directory tree

### Root `/`

```
/
├── bin          → usr/bin (symlink)
├── boot/        — kernel + EFI boot files (initrd, vmlinuz)
├── data/        — Portainer compose project definitions
│   └── compose/
│       ├── 1/   — nginx-proxy-manager-stack (data/, letsencrypt/)
│       └── 39/  — (nginx.conf — legacy/orphan config)
├── DATA/        — App data (not the same as /data)
│   └── AppData/
│       ├── html/    (owned www-data)
│       └── mysql/   (owned 999/root)
├── dev/
├── etc/
├── home/
│   ├── docker/       (root-owned)
│   ├── dockerkor/    (dockerkor user, locked down)
│   └── kor/          (main user — see below)
├── lib          → usr/lib (symlink)
├── lib64        → usr/lib64 (symlink)
├── lost+found/
├── media/
├── mnt/
│   └── storage/     — 469 GB ext4 SSD (see §4)
├── opt/
│   └── containerd/
├── portainer/
│   └── Files/
│       └── AppData/
├── proc/
├── root/        (root home, locked)
├── run/
├── sbin         → usr/sbin (symlink)
├── srv/
├── sys/
├── tmp/
├── usr/
└── var/
```

### `/home/kor/` (main user home)

```
/home/kor/
├── .bash_history
├── .bash_logout
├── .bashrc
├── .config/
├── dekstop2/       (typo; root-owned)
├── dekstop3/       (typo; root-owned)
├── desktop1/       (root-owned)
├── desktop2/       (root-owned)
├── .local/
├── .npm/
├── .profile
└── .ssh/
```

> Note: `dekstop2`, `dekstop3`, `desktop1`, `desktop2` are root-owned bind-mount
> staging dirs for noVNC desktop containers. The typos (`dekstop`) are in the
> actual filesystem.

### `/mnt/storage/` (secondary SSD)

```
/mnt/storage/
└── lost+found/    (root-owned; only contents — drive is essentially empty)
```

### `/data/compose/` (Portainer-managed stacks)

```
/data/compose/
├── 1/             — nginx-proxy-manager-stack
│   ├── data/
│   └── letsencrypt/
└── 39/            — legacy/orphan nginx config
    └── nginx.conf
```

> Note: `docker compose ls` references additional paths (`/data/compose/10/`,
> `/data/compose/97/`) for the `cloudflare` and `uuuuuu` stacks — those dirs may
> be managed in memory by Portainer without corresponding on-disk directories at
> probe time.

---

## 6. Docker stack

| Item | Value |
|---|---|
| Docker Engine | 29.5.0 (build 98f1464) |
| Docker Compose | v5.1.3 (plugin: `docker compose …`) |
| Management UI | Portainer CE (`/portainer/Files/AppData/`) |

### Running containers (4 — all healthy, up 10 days)

| Container | Image | Status | Host ports | Network(s) |
|---|---|---|---|---|
| `nginx-proxy-manager-stack-app-1` | `jc21/nginx-proxy-manager:latest` | Up 10 days | 80, 81 (admin UI), 443 | `nginx-proxy-manager-stack_default`, `uuuuuu_default` |
| `portainer` | `portainer/portainer-ce:latest` | Up 10 days | **8000**, 9443 (9000 internal) | `bridge`, `uuuuuu_default` |
| `cloudflare-cloudflare-ddns-1` | `oznu/cloudflare-ddns:latest` | Up 10 days | none | `cloudflare_default` |
| `ubuntu-novnc-desktop` | `dorowu/ubuntu-desktop-lxde-vnc:latest` | Up 10 days (healthy) | 5900 (VNC), 6080 (noVNC web) | `uuuuuu_default` |

> Change from last probe (2026-06-14): all previously paused/exited containers
> (`u2`, `u3_desktop1`, `u4_d2`, `onestopsold-web`, `korgems-web`, `terminal`)
> are **gone** — cleaned up. Only 4 running containers remain.
>
> **Host ports already in use: 80, 81, 443 (NPM); 8000, 9443 (Portainer);
> 5900, 6080 (noVNC).** New services must publish on a *different* host port —
> in particular **8000 is taken by Portainer**.

### Docker Compose projects

| Project | Status | Config file |
|---|---|---|
| `nginx-proxy-manager-stack` | running(1) | `/data/compose/1/docker-compose.yml` |
| `cloudflare` | running(1) | `/data/compose/10/docker-compose.yml` |
| `uuuuuu` | running(1) | `/data/compose/97/docker-compose.yml` |

### Docker volumes

| Driver | Name |
|---|---|
| local | `portainer_data` |
| local | `uuuuuu_ubuntu-desktop-data` |

### Docker networks

| Network | Driver | Notes |
|---|---|---|
| `bridge` | bridge | default; hosts `portainer` |
| `cloudflare_default` | bridge | hosts `cloudflare-cloudflare-ddns-1` |
| `nginx-proxy-manager-stack_default` | bridge | hosts NPM |
| `uuuuuu_default` | bridge | shared net: `ubuntu-novnc-desktop` + NPM + portainer |
| `host` | host | (no containers) |
| `none` | null | (no containers) |

### Reverse proxy — how NPM actually routes (from the Proxy Hosts list)

NPM is the public entry point (host ports 80/443; admin UI on **81**, reachable at
`http://100.90.62.96:81` over Tailscale when remote, or `http://192.168.0.150:81`
on the LAN). It routes `*.korgems.com` / `korwants.com` via Cloudflare DDNS.

Important: NPM here does **not** proxy by Docker network/container name — every
Proxy Host forwards to the box's **LAN IP + a published host port**:

| Source (domain) | Destination |
|---|---|
| `n.korwants.com` | `http://192.168.0.150:80` |
| `u.korgems.com` | `http://192.168.0.150:6080` (noVNC desktop) |
| `love.korgems.com` (planned) | `http://192.168.0.150:8100` (Love container) |

**To expose a new container through NPM:**
1. In its compose, **publish the container's port on a free host port** (avoid
   80/81/443/8000/9443/5900/6080). Love uses **`8100:8000`** (host 8100 →
   container 8000).
2. In the NPM admin UI, add a **Proxy Host** → forward to scheme `http`, host
   `192.168.0.150`, the chosen port. Turn on **Websockets Support**; for streaming
   apps add `proxy_buffering off;` in the Advanced tab; request a Let's Encrypt
   cert and Force SSL.
3. Add the Cloudflare DNS record (same pattern as the existing subdomains) so the
   domain resolves.

This matches the working setup already in place — **no changes to the NPM,
Portainer, or DDNS containers, and no shared Docker network needed.**

---

## 7. Guardrails for the next Claude (from homelab AGENTS rules)

- **Show commands before running them**; get approval for shell actions.
- **Snapshot / back up before changing anything** on the server.
- **Never commit secrets** — read from `.env` (gitignored). Don't print keys/tokens.
- **SSH via key auth** as `kor` over Tailscale (`100.90.62.96`), never passwords.
- **Destructive actions** (delete, drop, `rm -rf`, format, kernel/hypervisor
  changes) require the human to type `CONFIRM` first.
- **Do not** touch router config, open ports on the router, disable firewalls, or
  install kernels/hypervisors (e.g. Proxmox) without explicit approval.
- Passwordless sudo is **temporarily** enabled for `kor`
  (`/etc/sudoers.d/99-kor-nopasswd`) — slated for removal later.
- Keep `homelab.md` updated when the architecture changes.

---

## 8. Exact commands used for this probe (all read-only)

```bash
date && uptime && df -h && free -h
lsblk -f
ls -la / && ls -la /home/ && ls -la /home/kor/
ls -la /mnt/ && ls -la /mnt/storage/
ls -la /data/ && ls -la /DATA/ && ls -la /portainer/ && ls -la /opt/
ls -la /data/compose/ && ls /data/compose/*/
docker ps -a
docker volume ls && docker network ls
docker compose ls
ls -la /DATA/AppData/ && ls -la /portainer/Files/
```

---

### TL;DR for the new sandbox

Debian 13 N100 box named **`kor`**. **Two addresses:** SSH/admin from your desktop
over **Tailscale `100.90.62.96`**; NPM forwards to services on the box at the
**LAN IP `192.168.0.150:<host-port>`** (don't swap these). **4 cores, 16 GB RAM,
no GPU** → use **OpenRouter** for AI, not local big models. Docker **29.5.0** +
Compose **v5.1.3**, managed by **Portainer**. Four services up (10 days uptime):
**NPM** (:80/:443, admin :81), **Portainer** (**:8000**/:9443), **Cloudflare
DDNS**, **noVNC** (:6080) — so **host 8000 is taken**, pick another port. Old
paused/stopped containers are gone — the stack is clean. Put new AI containers'
data on **`/mnt/storage`** (~445 GB free, sda1) or **`/home`** (~801 GB free,
nvme0n1p4), publish on a free host port, and expose via a Proxy Host forwarding to
`192.168.0.150:<port>`. The OS partition (`/`, ~56 GB, **67% used**) should not
receive app data. **Love → `/mnt/storage/love`, host port `8100`,
`love.korgems.com` → `192.168.0.150:8100`.**

Last probed: **Mon Jun 15, 2026, 12:15 PM MST (Phoenix, AZ)**
