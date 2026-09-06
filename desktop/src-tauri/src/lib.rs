mod commands;

use commands::{
    connect_database, get_database_table_schema, list_database_tables, list_source_types, start_job,
    DatabaseSession,
};

#[tauri::command]
async fn ping(name: String) -> String {
    format!("Rust 后端已收到: {name}")
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // Initialize shared logging for the desktop process. Core command-specific
    // initialization remains owned by the CLI/runtime paths that need it.
    let _ = relus_common::logging::init_file_logger();
    // Load the shared application configuration for desktop commands. The CLI
    // controls watcher startup per command, so Tauri only initializes config.
    let _ = relus_core::init_system_config();

    tauri::Builder::default()
        .manage(DatabaseSession::default())
        .invoke_handler(tauri::generate_handler![
            ping,
            connect_database,
            list_database_tables,
            get_database_table_schema,
            list_source_types,
            start_job
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
