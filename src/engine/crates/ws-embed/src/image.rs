//! Reference image sizing and Pillow-compatible RGB bicubic resampling.
//! Ported from DigUp ImageSizing.swift (MIT); attribution is in THIRD-PARTY-NOTICES.
use crate::runtime::EmbedError;

pub(crate) fn prepare(
    rgb: &[u8],
    width: u32,
    height: u32,
    budget: u32,
) -> Result<(Vec<u8>, u32, u32), EmbedError> {
    let invalid =
        || EmbedError::MtmdFailed("invalid image dimensions, RGB buffer or token budget".into());
    let (w, h) = (width as usize, height as usize);
    let pixels = w.checked_mul(h).ok_or_else(invalid)?;
    if !matches!(budget, 70 | 140 | 280)
        || w == 0
        || h == 0
        || w > 16384
        || h > 16384
        || pixels > 16_777_216
        || pixels.checked_mul(3) != Some(rgb.len())
    {
        return Err(invalid());
    }
    let scale = ((budget as f64 * 48.0 * 48.0) / pixels as f64).sqrt();
    let mut tw = (scale * w as f64 / 48.0).floor() as usize * 48;
    let mut th = (scale * h as f64 / 48.0).floor() as usize * 48;
    if th == 0 {
        th = 48;
        tw = (w / h * 48).min(budget as usize * 48);
    }
    if tw == 0 {
        tw = 48;
        th = (h / w * 48).min(budget as usize * 48);
    }
    if tw == 0 || th == 0 || tw * th > budget as usize * 48 * 48 {
        return Err(invalid());
    }
    let horizontal = if tw == w {
        rgb.to_vec()
    } else {
        resize_axis(rgb, w, h, tw, true)
    };
    let resized = if th == h {
        horizontal
    } else {
        resize_axis(&horizontal, tw, h, th, false)
    };
    Ok((resized, tw as u32, th as u32))
}

fn filter(value: f64) -> f64 {
    let x = value.abs();
    if x < 1.0 {
        (1.5 * x - 2.5) * x * x + 1.0
    } else if x < 2.0 {
        ((x - 5.0) * x + 8.0) * x * -0.5 + 2.0
    } else {
        0.0
    }
}

fn resize_axis(
    src: &[u8],
    width: usize,
    height: usize,
    output: usize,
    horizontal: bool,
) -> Vec<u8> {
    let input = if horizontal { width } else { height };
    let scale = input as f64 / output as f64;
    let filter_scale = scale.max(1.0);
    let support = 2.0 * filter_scale;
    let coefficients: Vec<(usize, Vec<i64>)> = (0..output)
        .map(|o| {
            let center = (o as f64 + 0.5) * scale;
            let first = ((center - support + 0.5) as isize).max(0) as usize;
            let last = ((center + support + 0.5) as usize).min(input);
            let weights: Vec<f64> = (first..last)
                .map(|x| filter((x as f64 - center + 0.5) / filter_scale))
                .collect();
            let sum: f64 = weights.iter().sum();
            (
                first,
                weights
                    .into_iter()
                    .map(|v| {
                        let v = if sum != 0.0 { v / sum } else { v };
                        (v * (1 << 22) as f64 + if v < 0.0 { -0.5 } else { 0.5 }) as i64
                    })
                    .collect(),
            )
        })
        .collect();
    let (ow, oh) = if horizontal {
        (output, height)
    } else {
        (width, output)
    };
    let mut dst = vec![0u8; ow * oh * 3];
    for y in 0..oh {
        for x in 0..ow {
            let (first, weights) = &coefficients[if horizontal { x } else { y }];
            for c in 0..3 {
                let mut sum = 1i64 << 21;
                for (k, weight) in weights.iter().enumerate() {
                    let index = if horizontal {
                        (y * width + first + k) * 3 + c
                    } else {
                        ((first + k) * width + x) * 3 + c
                    };
                    sum += src[index] as i64 * weight;
                }
                dst[(y * ow + x) * 3 + c] = (sum >> 22).clamp(0, 255) as u8;
            }
        }
    }
    dst
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bicubic_matches_independent_pillow_pixels() {
        let src = [0, 0, 0, 255, 0, 0, 0, 255, 0, 0, 0, 255];
        let horizontal = resize_axis(&src, 2, 2, 4, true);
        let actual = resize_axis(&horizontal, 4, 2, 3, false);
        assert_eq!(
            actual,
            [
                0, 0, 0, 56, 0, 0, 215, 0, 0, 255, 0, 0, 0, 128, 0, 27, 101, 27, 101, 27, 101, 128,
                0, 128, 0, 255, 0, 0, 215, 56, 0, 56, 215, 0, 0, 255
            ]
        );
    }
    #[test]
    fn budgets_fill_aligned_pixels_and_bound_invalid_inputs() {
        for budget in [70, 140, 280] {
            let (rgb, w, h) = prepare(&[127; 6], 2, 1, budget).unwrap();
            assert_eq!((w % 48, h % 48), (0, 0));
            assert!(w * h <= budget * 48 * 48);
            assert_eq!(rgb.len(), (w * h * 3) as usize);
            assert!(rgb.iter().all(|v| *v == 127));
        }
        assert!(prepare(&[], u32::MAX, u32::MAX, 280).is_err());
        assert!(prepare(&[1, 2, 3], 1, 1, 71).is_err());
        assert!(prepare(&[1, 2], 1, 1, 280).is_err());
        let (_, w, h) = prepare(&vec![0; 4096 * 3], 4096, 1, 280).unwrap();
        assert_eq!((w, h), (13440, 48));
    }
}
