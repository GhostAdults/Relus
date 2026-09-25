use relus_mysql_cdc_candidate::verify_available_runtime;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let report = verify_available_runtime().await;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if report.has_failures() {
        std::process::exit(1);
    }
    Ok(())
}
