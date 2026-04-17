use std::sync::OnceLock;

static INITIALIZED: OnceLock<()> = OnceLock::new();

pub fn ensure_initialized() {
    INITIALIZED.get_or_init(|| {
        relus_reader::ReaderRegistry::collect_and_register();
        relus_writer::WriterRegistry::collect_and_register();
    });
}
