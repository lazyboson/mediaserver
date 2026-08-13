pub const PCMU_SILENCE: u8 = 0xFF;
pub const PCMA_SILENCE: u8 = 0xD5;

const ULAW_BIAS: i32 = 0x84;
const ULAW_CLIP: i32 = 32635;

fn msb(v: i32) -> i32 {
    31 - (v as u32).leading_zeros() as i32
}

pub fn linear_to_ulaw(sample: i16) -> u8 {
    let mut s = sample as i32;
    let sign: u8 = if s < 0 {
        s = -s;
        0x80
    } else {
        0
    };
    if s > ULAW_CLIP {
        s = ULAW_CLIP;
    }
    s += ULAW_BIAS;
    let exponent = (msb(s) - 7).clamp(0, 7) as u8;
    let mantissa = ((s >> (exponent + 3)) & 0x0F) as u8;
    !(sign | (exponent << 4) | mantissa)
}

pub fn ulaw_to_linear(byte: u8) -> i16 {
    let b = !byte;
    let sign = b & 0x80;
    let exponent = (b >> 4) & 0x07;
    let mantissa = (b & 0x0F) as i32;
    let sample = (((mantissa << 3) + ULAW_BIAS) << exponent) - ULAW_BIAS;
    if sign != 0 {
        -sample as i16
    } else {
        sample as i16
    }
}

pub fn linear_to_alaw(sample: i16) -> u8 {
    let s = sample as i32;
    let (sign, mut mag): (u8, i32) = if s >= 0 { (0x80, s) } else { (0, -s - 1) };
    if mag > 32767 {
        mag = 32767;
    }
    let compressed: u8 = if mag < 256 {
        (mag >> 4) as u8
    } else {
        let exponent = (msb(mag) - 7).clamp(1, 7) as u8;
        let mantissa = ((mag >> (exponent + 3)) & 0x0F) as u8;
        (exponent << 4) | mantissa
    };
    (compressed | sign) ^ 0x55
}

pub fn alaw_to_linear(byte: u8) -> i16 {
    let b = byte ^ 0x55;
    let sign = b & 0x80;
    let exponent = (b >> 4) & 0x07;
    let mantissa = (b & 0x0F) as i32;
    let mag = if exponent == 0 {
        (mantissa << 4) + 8
    } else {
        ((mantissa << 4) + 0x108) << (exponent - 1)
    };
    if sign != 0 {
        mag as i16
    } else {
        (-mag) as i16
    }
}

pub fn decode_into(ulaw: bool, payload: &[u8], out: &mut [i16]) -> usize {
    let n = payload.len().min(out.len());
    let f = if ulaw { ulaw_to_linear } else { alaw_to_linear };
    for (i, &b) in payload[..n].iter().enumerate() {
        out[i] = f(b);
    }
    n
}

pub fn encode_into(ulaw: bool, samples: &[i16], out: &mut [u8]) -> usize {
    let n = samples.len().min(out.len());
    let f = if ulaw { linear_to_ulaw } else { linear_to_alaw };
    for (i, &s) in samples[..n].iter().enumerate() {
        out[i] = f(s);
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ulaw_roundtrip_error_bounded_by_segment_step() {
        for s in i16::MIN..=i16::MAX {
            let decoded = ulaw_to_linear(linear_to_ulaw(s)) as i32;
            let mag = (s as i32).abs().min(ULAW_CLIP);
            let step = 8i32 << (msb((mag + ULAW_BIAS).max(0x84)) - 7).clamp(0, 7);
            assert!(
                (decoded - s as i32).abs() <= step,
                "s={s} decoded={decoded} step={step}"
            );
        }
    }

    #[test]
    fn alaw_roundtrip_error_bounded_by_segment_step() {
        for s in i16::MIN..=i16::MAX {
            let decoded = alaw_to_linear(linear_to_alaw(s)) as i32;
            let mag = (s as i32).abs().min(32767);
            let step = if mag < 256 {
                16
            } else {
                16i32 << ((msb(mag) - 7).clamp(1, 7) - 1)
            };
            assert!(
                (decoded - s as i32).abs() <= step,
                "s={s} decoded={decoded} step={step}"
            );
        }
    }

    #[test]
    fn encoders_are_monotonic_on_magnitude() {
        let mut prev_u = i32::MIN;
        let mut prev_a = i32::MIN;
        for s in (i16::MIN..=i16::MAX).step_by(7) {
            let du = ulaw_to_linear(linear_to_ulaw(s)) as i32;
            let da = alaw_to_linear(linear_to_alaw(s)) as i32;
            assert!(du >= prev_u, "ulaw not monotonic at {s}");
            assert!(da >= prev_a, "alaw not monotonic at {s}");
            prev_u = du;
            prev_a = da;
        }
    }

    #[test]
    fn silence_bytes_decode_near_zero() {
        assert!(ulaw_to_linear(PCMU_SILENCE).unsigned_abs() <= 8);
        assert!(alaw_to_linear(PCMA_SILENCE).unsigned_abs() <= 8);
    }

    #[test]
    fn all_bytes_decode_and_reencode_stably() {
        for byte in 0u8..=255 {
            let l = ulaw_to_linear(byte);
            let b1 = linear_to_ulaw(l);
            assert_eq!(ulaw_to_linear(b1), l, "ulaw byte {byte:#x} value drifted");
            assert_eq!(
                linear_to_ulaw(ulaw_to_linear(b1)),
                b1,
                "ulaw byte {byte:#x} not stable"
            );
        }
        for byte in 0u8..=255 {
            let l = alaw_to_linear(byte);
            let b1 = linear_to_alaw(l);
            assert_eq!(alaw_to_linear(b1), l, "alaw byte {byte:#x} value drifted");
            assert_eq!(
                linear_to_alaw(alaw_to_linear(b1)),
                b1,
                "alaw byte {byte:#x} not stable"
            );
        }
    }

    #[test]
    fn bulk_helpers() {
        let payload = [PCMU_SILENCE; 160];
        let mut pcm = [0i16; 160];
        assert_eq!(decode_into(true, &payload, &mut pcm), 160);
        let mut back = [0u8; 160];
        assert_eq!(encode_into(true, &pcm, &mut back), 160);
        assert_eq!(back, payload);
    }
}
