//! Minimal Matrix client-server API client (v3). Unencrypted rooms only.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tauri::{AppHandle, Emitter};

const SYNC_FILTER: &str = r#"{"room":{"timeline":{"limit":40},"state":{"lazy_load_members":true},"ephemeral":{"types":[]},"account_data":{"types":[]}},"presence":{"types":[]},"account_data":{"types":[]}}"#;

#[derive(Serialize, Deserialize, Clone)]
pub struct Session {
    pub homeserver: String,
    pub user_id: String,
    pub access_token: String,
}

#[derive(Serialize, Clone)]
pub struct RoomSummary {
    pub id: String,
    pub name: String,
    pub invited: bool,
    pub preview: String,
    pub ts: u64,
}

#[derive(Serialize, Clone)]
pub struct MessageOut {
    pub id: String,
    pub sender: String,
    pub sender_name: String,
    pub body: String,
    pub ts: u64,
    pub mine: bool,
}

struct Msg {
    id: String,
    sender: String,
    body: String,
    ts: u64,
}

#[derive(Default)]
struct Room {
    name: Option<String>,
    alias: Option<String>,
    heroes: Vec<String>,
    members: HashMap<String, String>,
    messages: Vec<Msg>,
    invited: bool,
}

#[derive(Default)]
struct Inner {
    session: Option<Session>,
    rooms: HashMap<String, Room>,
    generation: u64,
}

pub struct Matrix {
    inner: Arc<Mutex<Inner>>,
    http: Client,
    txn: AtomicU64,
}

fn localpart(user: &str) -> String {
    user.trim_start_matches('@')
        .split(':')
        .next()
        .unwrap_or(user)
        .to_string()
}

fn enc(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn api_error(resp: reqwest::Response) -> String {
    let status = resp.status();
    let v: Value = resp.json().await.unwrap_or(Value::Null);
    v["error"]
        .as_str()
        .map(String::from)
        .unwrap_or_else(|| format!("HTTP {status}"))
}

async fn nap(backoff: &mut u64) {
    tokio::time::sleep(Duration::from_secs(*backoff)).await;
    *backoff = (*backoff * 2).min(30);
}

/// Accepts "matrix.org" or "https://host"; follows .well-known when given a bare domain.
async fn discover(http: &Client, input: &str) -> String {
    let t = input.trim().trim_end_matches('/');
    if t.starts_with("http://") || t.starts_with("https://") {
        return t.to_string();
    }
    let base = format!("https://{t}");
    if let Ok(r) = http
        .get(format!("{base}/.well-known/matrix/client"))
        .send()
        .await
    {
        if let Ok(v) = r.json::<Value>().await {
            if let Some(u) = v["m.homeserver"]["base_url"].as_str() {
                return u.trim_end_matches('/').to_string();
            }
        }
    }
    base
}

impl Room {
    fn display_name(&self, id: &str, me: &str) -> String {
        if let Some(n) = &self.name {
            return n.clone();
        }
        if let Some(a) = &self.alias {
            return a.clone();
        }
        let names: Vec<String> = self
            .heroes
            .iter()
            .filter(|h| h.as_str() != me)
            .map(|h| self.members.get(h).cloned().unwrap_or_else(|| localpart(h)))
            .collect();
        if !names.is_empty() {
            return names.join(", ");
        }
        let mut others: Vec<String> = self
            .members
            .iter()
            .filter(|(u, _)| u.as_str() != me)
            .map(|(_, n)| n.clone())
            .collect();
        others.sort();
        if !others.is_empty() {
            return others.into_iter().take(3).collect::<Vec<_>>().join(", ");
        }
        id.to_string()
    }
}

fn handle_event(room: &mut Room, ev: &Value) {
    let content = &ev["content"];
    match ev["type"].as_str().unwrap_or("") {
        "m.room.name" => {
            room.name = content["name"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(String::from);
        }
        "m.room.canonical_alias" => {
            room.alias = content["alias"].as_str().map(String::from);
        }
        "m.room.member" => {
            if let Some(user) = ev["state_key"].as_str() {
                if content["membership"].as_str() == Some("join") {
                    let name = content["displayname"]
                        .as_str()
                        .map(String::from)
                        .unwrap_or_else(|| localpart(user));
                    room.members.insert(user.to_string(), name);
                } else {
                    room.members.remove(user);
                }
            }
        }
        "m.room.message" => {
            if content["body"].is_null()
                || content["m.relates_to"]["rel_type"].as_str() == Some("m.replace")
            {
                return;
            }
            let id = ev["event_id"].as_str().unwrap_or("").to_string();
            if room.messages.iter().any(|m| m.id == id) {
                return;
            }
            let msgtype = content["msgtype"].as_str().unwrap_or("m.text");
            let raw = content["body"].as_str().unwrap_or("");
            let body = match msgtype {
                "m.text" | "m.notice" | "m.emote" => raw.to_string(),
                other => format!("[{}] {}", other.trim_start_matches("m."), raw),
            };
            room.messages.push(Msg {
                id,
                sender: ev["sender"].as_str().unwrap_or("").to_string(),
                body,
                ts: ev["origin_server_ts"].as_u64().unwrap_or(0),
            });
            if room.messages.len() > 500 {
                room.messages.drain(0..100);
            }
        }
        _ => {}
    }
}

/// Returns false if this sync loop has been superseded.
fn apply_sync(inner: &Arc<Mutex<Inner>>, gen: u64, v: &Value) -> bool {
    let mut g = inner.lock().unwrap();
    if g.generation != gen {
        return false;
    }
    if let Some(join) = v["rooms"]["join"].as_object() {
        for (id, r) in join {
            let room = g.rooms.entry(id.clone()).or_default();
            room.invited = false;
            if let Some(h) = r["summary"]["m.heroes"].as_array() {
                room.heroes = h
                    .iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect();
            }
            for key in ["state", "timeline"] {
                if let Some(evs) = r[key]["events"].as_array() {
                    for ev in evs {
                        handle_event(room, ev);
                    }
                }
            }
        }
    }
    if let Some(invite) = v["rooms"]["invite"].as_object() {
        for (id, r) in invite {
            let room = g.rooms.entry(id.clone()).or_default();
            room.invited = true;
            if let Some(evs) = r["invite_state"]["events"].as_array() {
                for ev in evs {
                    handle_event(room, ev);
                }
            }
        }
    }
    if let Some(leave) = v["rooms"]["leave"].as_object() {
        for id in leave.keys() {
            g.rooms.remove(id);
        }
    }
    true
}

impl Matrix {
    pub fn new() -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .expect("http client");
        Matrix {
            inner: Arc::new(Mutex::new(Inner::default())),
            http,
            txn: AtomicU64::new(0),
        }
    }

    fn session(&self) -> Result<Session, String> {
        self.inner
            .lock()
            .unwrap()
            .session
            .clone()
            .ok_or_else(|| "Not signed in".to_string())
    }

    pub async fn login(&self, server: &str, user: &str, password: &str) -> Result<Session, String> {
        let homeserver = discover(&self.http, server).await;
        let resp = self
            .http
            .post(format!("{homeserver}/_matrix/client/v3/login"))
            .json(&json!({
                "type": "m.login.password",
                "identifier": { "type": "m.id.user", "user": user.trim() },
                "password": password,
                "initial_device_display_name": "Ferrum iOS"
            }))
            .send()
            .await
            .map_err(|e| format!("Cannot reach {homeserver}: {e}"))?;
        if !resp.status().is_success() {
            return Err(api_error(resp).await);
        }
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let access_token = v["access_token"]
            .as_str()
            .ok_or("Malformed login response")?
            .to_string();
        let user_id = v["user_id"]
            .as_str()
            .ok_or("Malformed login response")?
            .to_string();
        Ok(Session {
            homeserver,
            user_id,
            access_token,
        })
    }

    pub fn set_session(&self, s: Session) {
        let mut g = self.inner.lock().unwrap();
        g.session = Some(s);
        g.rooms.clear();
    }

    pub fn start_sync(&self, app: AppHandle) {
        let gen = {
            let mut g = self.inner.lock().unwrap();
            g.generation += 1;
            g.generation
        };
        let inner = self.inner.clone();
        let http = self.http.clone();
        tauri::async_runtime::spawn(async move {
            let mut since: Option<String> = None;
            let mut backoff = 1u64;
            loop {
                let session = {
                    let g = inner.lock().unwrap();
                    if g.generation != gen {
                        return;
                    }
                    match &g.session {
                        Some(s) => s.clone(),
                        None => return,
                    }
                };
                let timeout = if since.is_some() { "30000" } else { "0" };
                let mut req = http
                    .get(format!("{}/_matrix/client/v3/sync", session.homeserver))
                    .bearer_auth(&session.access_token)
                    .query(&[("timeout", timeout), ("filter", SYNC_FILTER)]);
                if let Some(s) = &since {
                    req = req.query(&[("since", s.as_str())]);
                }
                let resp = match req.send().await {
                    Ok(r) => r,
                    Err(_) => {
                        nap(&mut backoff).await;
                        continue;
                    }
                };
                if resp.status().as_u16() == 401 {
                    let _ = app.emit("matrix-signed-out", 0);
                    return;
                }
                if !resp.status().is_success() {
                    nap(&mut backoff).await;
                    continue;
                }
                let v: Value = match resp.json().await {
                    Ok(v) => v,
                    Err(_) => {
                        nap(&mut backoff).await;
                        continue;
                    }
                };
                backoff = 1;
                if !apply_sync(&inner, gen, &v) {
                    return;
                }
                since = v["next_batch"].as_str().map(String::from);
                let _ = app.emit("matrix-update", 0);
            }
        });
    }

    pub fn rooms(&self) -> Vec<RoomSummary> {
        let g = self.inner.lock().unwrap();
        let me = g
            .session
            .as_ref()
            .map(|s| s.user_id.clone())
            .unwrap_or_default();
        let mut out: Vec<RoomSummary> = g
            .rooms
            .iter()
            .map(|(id, r)| {
                let last = r.messages.last();
                RoomSummary {
                    id: id.clone(),
                    name: r.display_name(id, &me),
                    invited: r.invited,
                    preview: last
                        .map(|m| m.body.chars().take(80).collect::<String>())
                        .unwrap_or_default(),
                    ts: last.map(|m| m.ts).unwrap_or(0),
                }
            })
            .collect();
        out.sort_by(|a, b| b.invited.cmp(&a.invited).then(b.ts.cmp(&a.ts)));
        out
    }

    pub fn messages(&self, room_id: &str) -> Vec<MessageOut> {
        let g = self.inner.lock().unwrap();
        let me = g
            .session
            .as_ref()
            .map(|s| s.user_id.clone())
            .unwrap_or_default();
        let Some(room) = g.rooms.get(room_id) else {
            return vec![];
        };
        room.messages
            .iter()
            .map(|m| MessageOut {
                id: m.id.clone(),
                sender: m.sender.clone(),
                sender_name: room
                    .members
                    .get(&m.sender)
                    .cloned()
                    .unwrap_or_else(|| localpart(&m.sender)),
                body: m.body.clone(),
                ts: m.ts,
                mine: m.sender == me,
            })
            .collect()
    }

    pub async fn send(&self, room_id: &str, body: &str) -> Result<(), String> {
        let s = self.session()?;
        let txn = format!(
            "ferrum-{}-{}",
            now_ms(),
            self.txn.fetch_add(1, Ordering::Relaxed)
        );
        let url = format!(
            "{}/_matrix/client/v3/rooms/{}/send/m.room.message/{}",
            s.homeserver,
            enc(room_id),
            txn
        );
        let resp = self
            .http
            .put(url)
            .bearer_auth(&s.access_token)
            .json(&json!({ "msgtype": "m.text", "body": body }))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(api_error(resp).await)
        }
    }

    pub async fn join(&self, room: &str) -> Result<(), String> {
        let s = self.session()?;
        let resp = self
            .http
            .post(format!(
                "{}/_matrix/client/v3/join/{}",
                s.homeserver,
                enc(room.trim())
            ))
            .bearer_auth(&s.access_token)
            .json(&json!({}))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(api_error(resp).await)
        }
    }

    pub async fn leave(&self, room_id: &str) -> Result<(), String> {
        let s = self.session()?;
        let resp = self
            .http
            .post(format!(
                "{}/_matrix/client/v3/rooms/{}/leave",
                s.homeserver,
                enc(room_id)
            ))
            .bearer_auth(&s.access_token)
            .json(&json!({}))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if resp.status().is_success() {
            self.inner.lock().unwrap().rooms.remove(room_id);
            Ok(())
        } else {
            Err(api_error(resp).await)
        }
    }

    pub async fn logout(&self) {
        let s = {
            let mut g = self.inner.lock().unwrap();
            g.generation += 1;
            g.rooms.clear();
            g.session.take()
        };
        if let Some(s) = s {
            let _ = self
                .http
                .post(format!("{}/_matrix/client/v3/logout", s.homeserver))
                .bearer_auth(&s.access_token)
                .json(&json!({}))
                .send()
                .await;
        }
    }
}
