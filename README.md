# Ferrum

A small Matrix client for iOS. Rust backend (Matrix client-server API over reqwest + rustls),
Tauri 2 shell, plain HTML/JS frontend (no npm needed).

Features: password login, session restore, room list, invites (accept/decline), join by alias/ID,
send/receive text messages via long-poll `/sync`. Unencrypted rooms only (no E2EE yet).

## Build an IPA on GitHub
1. Change `identifier` in `src-tauri/tauri.conf.json` to your own bundle ID.
2. Push this repo to GitHub.
3. Actions tab -> **iOS IPA (unsigned)** -> Run workflow -> download the artifact.
   Install it by re-signing with Sideloadly / AltStore / TrollStore.
4. With a paid Apple Developer account, add the secrets listed in `ios-signed.yml` and run **iOS IPA (signed)**.

## Local desktop dev
    cargo install tauri-cli --version "^2" --locked
    cargo tauri icon app-icon.png
    cargo tauri dev
