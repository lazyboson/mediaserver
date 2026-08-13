use criterion::{criterion_group, criterion_main, Criterion};
use media_core::pipeline::{Playout, StreamPipeline};
use media_core::replay::G711StreamGenerator;
use media_core::AudioFormat;
use std::hint::black_box;

const TARGET_DEPTH_PACKETS: u16 = 3;
const STREAM_PACKETS: usize = 1000;

fn tapped_stream(format: AudioFormat) -> Vec<Vec<u8>> {
    let mut generator = G711StreamGenerator::new(format, 0x1234_5678, 0).unwrap();
    (0..STREAM_PACKETS)
        .map(|_| generator.next_datagram())
        .collect()
}

fn parse_jitter_decode(c: &mut Criterion) {
    let format = AudioFormat::pcmu_8k_20ms();
    let stream = tapped_stream(format);

    c.bench_function("parse_jitter_decode_per_packet", |b| {
        b.iter(|| {
            let mut pipeline =
                StreamPipeline::new(format, TARGET_DEPTH_PACKETS, Some(101)).unwrap();
            let mut samples = 0usize;
            for datagram in &stream {
                pipeline.ingest(black_box(datagram));
                match pipeline.release() {
                    Playout::Pcm(pcm) | Playout::Concealed(pcm) => samples += pcm.len(),
                    Playout::Waiting => {}
                }
            }
            black_box(samples)
        })
    });
}

fn ingest_only(c: &mut Criterion) {
    let format = AudioFormat::pcmu_8k_20ms();
    let stream = tapped_stream(format);

    c.bench_function("ingest_only_per_packet", |b| {
        b.iter(|| {
            let mut pipeline =
                StreamPipeline::new(format, TARGET_DEPTH_PACKETS, Some(101)).unwrap();
            for datagram in &stream {
                black_box(pipeline.ingest(black_box(datagram)));
            }
        })
    });
}

criterion_group!(benches, parse_jitter_decode, ingest_only);
criterion_main!(benches);
