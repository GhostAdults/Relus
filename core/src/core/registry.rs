pub use relus_common::interface::GlobalRegistry;

use std::sync::OnceLock;

static INITIALIZED: OnceLock<()> = OnceLock::new();

pub fn ensure_initialized() {
    INITIALIZED.get_or_init(|| {
        GlobalRegistry::collect_and_register();
    });
}
