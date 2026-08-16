fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = ["mediacontrol.proto", "mediastream.proto"];
    let include = "../../proto";

    let descriptors = protox::compile(protos, [include])?;

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_fds(descriptors)?;

    println!("cargo:rerun-if-changed={include}");
    for proto in protos {
        println!("cargo:rerun-if-changed={include}/{proto}");
    }
    Ok(())
}
