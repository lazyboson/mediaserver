use media_core::opus::{OpusStreamDecoder, MAX_OPUS_PACKET_BYTES};
use media_core::{AudioFormat, Encoding};
use opus_rs::{Application, OpusEncoder};

const USAGE: &str = "\
opus_speech_probe <input.wav> <output.wav> [sample-rate-hz]

Encodes real speech to opus and decodes it back through OpusStreamDecoder, so
the result can be judged by lab/ear_intelligibility_probe.py rather than by a
sine wave. The input wav must be 16-bit mono at the chosen rate.";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 2 {
        eprintln!("{USAGE}");
        std::process::exit(2);
    }
    let rate: u32 = args.get(2).map(|r| r.parse()).transpose()?.unwrap_or(16000);
    let frame = (rate / 1000 * 20) as usize;

    let mut reader = hound::WavReader::open(&args[0])?;
    let spec = reader.spec();
    if spec.channels != 1 || spec.sample_rate != rate {
        eprintln!(
            "probe: input is {} ch at {} Hz; this probe needs mono at {rate} Hz",
            spec.channels, spec.sample_rate
        );
        std::process::exit(2);
    }
    let pcm: Vec<i16> = reader.samples::<i16>().collect::<Result<_, _>>()?;
    println!("probe: {} samples in at {rate} Hz", pcm.len());

    let format = AudioFormat {
        encoding: Encoding::Opus,
        sample_rate_hz: rate,
        channels: 1,
        ptime_ms: 20,
    };
    let mut encoder =
        OpusEncoder::new(rate as i32, 1, Application::Voip).map_err(|e| format!("encoder: {e}"))?;
    encoder.bitrate_bps = 24000;
    encoder.use_cbr = true;
    let mut decoder = OpusStreamDecoder::new(format)?;

    let mut writer = hound::WavWriter::create(
        &args[1],
        hound::WavSpec {
            channels: 1,
            sample_rate: rate,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        },
    )?;

    let mut packet = vec![0u8; MAX_OPUS_PACKET_BYTES];
    let mut decoded = vec![0i16; decoder.max_frame_samples()];
    let mut frames = 0u64;
    let mut packet_bytes = 0u64;
    let mut samples_out = 0u64;

    for chunk in pcm.chunks(frame) {
        let mut input: Vec<f32> = chunk.iter().map(|s| f32::from(*s) / 32768.0).collect();
        input.resize(frame, 0.0);
        let bytes = encoder
            .encode(&input, frame, &mut packet)
            .map_err(|e| format!("encode: {e}"))?;
        packet_bytes += bytes as u64;
        let produced = decoder.decode(&packet[..bytes], &mut decoded)?;
        for sample in decoded.iter().take(produced) {
            writer.write_sample(*sample)?;
        }
        frames += 1;
        samples_out += produced as u64;
    }
    writer.finalize()?;

    let rms = |v: &[i16]| {
        let sum: f64 = v.iter().map(|s| f64::from(*s) * f64::from(*s)).sum();
        (sum / v.len().max(1) as f64).sqrt()
    };
    println!(
        "probe: {frames} frames, {packet_bytes} opus bytes ({:.1} kbps), {samples_out} samples out",
        packet_bytes as f64 * 8.0 / (samples_out as f64 / rate as f64) / 1000.0
    );
    println!(
        "probe: input rms {:.1} -> wrote {} at rms (read it back to compare)",
        rms(&pcm),
        args[1]
    );
    Ok(())
}
