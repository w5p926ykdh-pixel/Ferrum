mod matrix;

use matrix::{Matrix, MessageOut, RoomSummary, Session};
use std::path::PathBuf;
use tauri::{AppHandle, Manager, State};

fn session_path(app: &AppHandle) -> Result<PathBuf, String> {
    let dir = app.path().app_data_dir().map_err(|e| e.to_string())?;
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    Ok(dir.join("session.json"))
}

#[tauri::command]
async fn login(
    app: AppHandle,
    state: State<'_, Matrix>,
    server: String,
    user: String,
    password: String,
) -> Result<String, String> {
    let s = state.login(&server, &user, &password).await?;
    if let Ok(p) = session_path(&app) {
        let _ = std::fs::write(p, serde_json::to_string(&s).unwrap_or_default());
    }
    let uid = s.user_id.clone();
    state.set_session(s);
    state.start_sync(app);
    Ok(uid)
}

#[tauri::command]
fn restore(app: AppHandle, state: State<'_, Matrix>) -> Option<String> {
    let path = session_path(&app).ok()?;
    let txt = std::fs::read_to_string(path).ok()?;
    let s: Session = serde_json::from_str(&txt).ok()?;
    let uid = s.user_id.clone();
    state.set_session(s);
    state.start_sync(app);
    Some(uid)
}

#[tauri::command]
fn rooms(state: State<'_, Matrix>) -> Vec<RoomSummary> {
    state.rooms()
}

#[tauri::command]
fn messages(state: State<'_, Matrix>, room_id: String) -> Vec<MessageOut> {
    state.messages(&room_id)
}

#[tauri::command]
async fn send(state: State<'_, Matrix>, room_id: String, body: String) -> Result<(), String> {
    state.send(&room_id, &body).await
}

#[tauri::command]
async fn join(state: State<'_, Matrix>, room_id: String) -> Result<(), String> {
    state.join(&room_id).await
}

#[tauri::command]
async fn leave(state: State<'_, Matrix>, room_id: String) -> Result<(), String> {
    state.leave(&room_id).await
}

#[tauri::command]
async fn logout(app: AppHandle, state: State<'_, Matrix>) -> Result<(), String> {
    state.logout().await;
    if let Ok(p) = session_path(&app) {
        let _ = std::fs::remove_file(p);
    }
    Ok(())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(Matrix::new())
        .invoke_handler(tauri::generate_handler![
            login, restore, rooms, messages, send, join, leave, logout
        ])
        .run(tauri::generate_context!())
        .expect("error while running Ferrum");
}
