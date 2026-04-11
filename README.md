"# data-sync-trans"

# Relus

## Build

```bash
cargo build --release --bin data_trans_cli
```

## Usage

```bash
# debug
cargo run -p data_trans_cli -- sync -c data_trans_cli/user_config/default_job.json

# release
./target/release/data_trans_cli sync -c data_trans_cli/user_config/default_job.json
```
