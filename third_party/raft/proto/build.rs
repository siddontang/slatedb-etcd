fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=eraftpb.proto");
    prost_build::Config::new()
        .bytes([".eraftpb"])
        .compile_protos(&["eraftpb.proto"], &["."])
}
