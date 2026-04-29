fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/worker.proto")?;
    tonic_build::compile_protos("proto/flow_mgmt.proto")?;
    Ok(())
}