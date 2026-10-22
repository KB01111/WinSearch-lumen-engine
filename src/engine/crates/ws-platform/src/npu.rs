//! AMD Ryzen AI NPU discovery through the PnP device tree.
//!
//! Detection only reads present PCI device instance IDs
//! (`PCI\VEN_1022&DEV_17F0&...&REV_10\...`); it loads no driver code and
//! opens no device. A detected NPU is a candidate, not proof of inference:
//! the embed worker still opens it through XRT and falls back to GPU/CPU when
//! that fails.
//!
//! Device IDs follow the `amdxdna` driver's PCI table: `0x1502` is the
//! first-generation XDNA NPU (Phoenix, Hawk Point), `0x17F0` the XDNA 2 / AIE2P
//! NPU (Strix Point, Strix Halo, Krackan). IRON's "NPU2" kernels only run on
//! the latter.

const AMD_VENDOR_ID: u16 = 0x1022;

/// NPU generation, by PCI device ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NpuGeneration {
    /// XDNA (AIE2): Phoenix, Hawk Point. IRON NPU2 kernels do not load here.
    Npu1,
    /// XDNA 2 (AIE2P): Strix Point, Strix Halo, Krackan.
    Npu2,
}

/// One present AMD NPU.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NpuDevice {
    pub generation: NpuGeneration,
    pub device_id: u16,
    pub revision: Option<u8>,
    /// PnP instance ID (Windows) or sysfs PCI address (Linux), for logs.
    pub instance: String,
}

impl NpuDevice {
    /// Whether this NPU can run the IRON EmbeddingGemma 2 NPU2 bundle.
    pub fn runs_npu2_kernels(&self) -> bool {
        self.generation == NpuGeneration::Npu2
    }
}

/// Classifies a PCI vendor/device pair as an AMD NPU.
pub fn classify(vendor: u16, device: u16) -> Option<NpuGeneration> {
    match (vendor, device) {
        (AMD_VENDOR_ID, 0x1502) => Some(NpuGeneration::Npu1),
        (AMD_VENDOR_ID, 0x17F0) => Some(NpuGeneration::Npu2),
        _ => None,
    }
}

/// Parses `PCI\VEN_xxxx&DEV_xxxx[&SUBSYS_...][&REV_xx]\...` (case-insensitive).
pub fn parse_pci_instance_id(id: &str) -> Option<(u16, u16, Option<u8>)> {
    let upper = id.to_ascii_uppercase();
    let hardware = upper.strip_prefix("PCI\\")?.split('\\').next()?;
    let (mut ven, mut dev, mut rev) = (None, None, None);
    for part in hardware.split('&') {
        if let Some(v) = part.strip_prefix("VEN_") {
            ven = u16::from_str_radix(v, 16).ok();
        } else if let Some(v) = part.strip_prefix("DEV_") {
            dev = u16::from_str_radix(v, 16).ok();
        } else if let Some(v) = part.strip_prefix("REV_") {
            rev = u8::from_str_radix(v, 16).ok();
        }
    }
    Some((ven?, dev?, rev))
}

fn from_instance(instance: &str) -> Option<NpuDevice> {
    let (vendor, device_id, revision) = parse_pci_instance_id(instance)?;
    Some(NpuDevice {
        generation: classify(vendor, device_id)?,
        device_id,
        revision,
        instance: instance.to_string(),
    })
}

/// Every present AMD NPU, NPU2 first. Empty when none is present or the
/// device tree cannot be read.
pub fn detect() -> Vec<NpuDevice> {
    let mut found: Vec<NpuDevice> = present_pci_instances()
        .iter()
        .filter_map(|id| from_instance(id))
        .collect();
    found.sort_by_key(|d| d.generation != NpuGeneration::Npu2);
    found
}

/// The first NPU able to run the IRON NPU2 bundle, if any.
pub fn detect_npu2() -> Option<NpuDevice> {
    detect().into_iter().find(NpuDevice::runs_npu2_kernels)
}

#[cfg(windows)]
fn present_pci_instances() -> Vec<String> {
    use windows::core::w;
    use windows::Win32::Devices::DeviceAndDriverInstallation::{
        CM_Get_Device_ID_ListW, CM_Get_Device_ID_List_SizeW, CM_GETIDLIST_FILTER_ENUMERATOR,
        CM_GETIDLIST_FILTER_PRESENT, CR_BUFFER_SMALL, CR_SUCCESS,
    };
    let flags = CM_GETIDLIST_FILTER_ENUMERATOR | CM_GETIDLIST_FILTER_PRESENT;
    // The list can grow between the size query and the read (hot-plug).
    for _ in 0..3 {
        let mut len = 0u32;
        // SAFETY: len is a valid out pointer; the filter is a static wide string.
        if unsafe { CM_Get_Device_ID_List_SizeW(&mut len, w!("PCI"), flags) } != CR_SUCCESS {
            return Vec::new();
        }
        let mut buf = vec![0u16; len.max(2) as usize];
        // SAFETY: buf is writable for its whole length.
        match unsafe { CM_Get_Device_ID_ListW(w!("PCI"), &mut buf, flags) } {
            r if r == CR_SUCCESS => {
                return buf
                    .split(|c| *c == 0)
                    .filter(|s| !s.is_empty())
                    .map(String::from_utf16_lossy)
                    .collect();
            }
            r if r == CR_BUFFER_SMALL => continue,
            _ => return Vec::new(),
        }
    }
    Vec::new()
}

/// Linux development hosts: `/sys/bus/pci/devices/*/{vendor,device,revision}`
/// rendered as Windows-style instance IDs so one parser serves both.
#[cfg(not(windows))]
fn present_pci_instances() -> Vec<String> {
    let read_hex = |p: std::path::PathBuf| {
        std::fs::read_to_string(p)
            .ok()
            .map(|s| s.trim().trim_start_matches("0x").to_string())
    };
    let Ok(entries) = std::fs::read_dir("/sys/bus/pci/devices") else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|e| {
            let dir = e.path();
            let ven = read_hex(dir.join("vendor"))?;
            let dev = read_hex(dir.join("device"))?;
            let rev = read_hex(dir.join("revision")).unwrap_or_else(|| "00".into());
            Some(format!(
                "PCI\\VEN_{ven}&DEV_{dev}&REV_{rev}\\{}",
                e.file_name().to_string_lossy()
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_windows_instance_ids() {
        assert_eq!(
            parse_pci_instance_id(
                r"PCI\VEN_1022&DEV_17F0&SUBSYS_88DA103C&REV_10\4&2B4A6B1A&0&0141"
            ),
            Some((0x1022, 0x17F0, Some(0x10)))
        );
        assert_eq!(
            parse_pci_instance_id(r"pci\ven_1022&dev_1502&rev_00\x"),
            Some((0x1022, 0x1502, Some(0)))
        );
        assert_eq!(
            parse_pci_instance_id(r"PCI\VEN_10DE&DEV_2684\x"),
            Some((0x10DE, 0x2684, None))
        );
        assert_eq!(parse_pci_instance_id(r"USB\VID_046D&PID_C52B\x"), None);
        assert_eq!(parse_pci_instance_id(r"PCI\VEN_ZZZZ&DEV_17F0\x"), None);
        assert_eq!(parse_pci_instance_id(r"PCI\DEV_17F0\x"), None);
    }

    #[test]
    fn classifies_npu_generations() {
        assert_eq!(classify(0x1022, 0x17F0), Some(NpuGeneration::Npu2));
        assert_eq!(classify(0x1022, 0x1502), Some(NpuGeneration::Npu1));
        // An AMD GPU or another vendor's device with the same ID is not an NPU.
        assert_eq!(classify(0x1022, 0x150E), None);
        assert_eq!(classify(0x1002, 0x17F0), None);
    }

    #[test]
    fn only_npu2_runs_the_iron_bundle() {
        let npu2 = from_instance(r"PCI\VEN_1022&DEV_17F0&REV_10\a").unwrap();
        let npu1 = from_instance(r"PCI\VEN_1022&DEV_1502&REV_00\b").unwrap();
        assert!(npu2.runs_npu2_kernels());
        assert!(!npu1.runs_npu2_kernels());
        assert!(from_instance(r"PCI\VEN_8086&DEV_7D1D\c").is_none());
    }

    #[test]
    fn detection_never_panics_without_an_npu() {
        // Hosts without an NPU (CI) simply report none.
        let _ = detect();
        let _ = detect_npu2();
    }
}
