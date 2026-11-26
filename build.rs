fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .codec_path("crate::proto::codec::Codec")
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("services.pb"),
        )
        .message_attribute(".", "#[serde(default)]")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(
            &[
                "proto/rpc.proto",
                "proto/kv.proto",
                "proto/peer.proto",
                "proto/command.proto",
                "proto/state.proto",
            ],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
