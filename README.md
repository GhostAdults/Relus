"# data-sync-trans"

# Relus

## Build

cd Relus

```bash
cargo build --release --bin data_trans_cli
```
or
```bash
cargo packager --release
```

## Usage

```bash
# debug
cargo run -p data_trans_cli -- sync -c data_trans_cli/user_config/default_job.
# 或者
cargo run --manifest-path data_trans_cli/Cargo.toml -- sync -c xxxYourJobLocation/xxx/default_job.json

# release
./target/release//relus-cli sync -c xxxYourJobLocation/xxx/default_job.json
```
