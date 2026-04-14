"# data sync transport tools"

# Relus

## Build

cd Relus

```bash
cargo build --release --bin cli
```
or
```bash
cargo packager --release
```

## Usage

```bash
# debug
cargo run -p relus_cli -- sync -c cli/user_config/default_job.
# 或者
cargo run --manifest-path cli/Cargo.toml -- sync -c xxxYourJobLocation/xxx/default_job.json

# release
./target/release//relus-cli sync -c xxxYourJobLocation/xxx/default_job.json
```
