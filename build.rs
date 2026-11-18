fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .message_attribute(".", "#[serde(default)]")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(
            &["proto/rpc.proto", "proto/kv.proto", "proto/peer.proto"],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
