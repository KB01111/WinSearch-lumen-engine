use super::{contains, normalized};
use std::os::windows::fs::MetadataExt;
use std::path::Path;
use ws_index::StoreError;
use ws_protocol::{IndexingConfig, RootPolicy};

pub fn validate(config: &mut IndexingConfig) -> Result<(), StoreError> {
    let bad = || StoreError::InvalidConfig("invalid per-root source policy".into());
    if config.root_policies.len() > 256 {
        return Err(bad());
    }
    let mut seen = std::collections::HashSet::new();
    let mut exclusions = 0;
    for policy in &mut config.root_policies {
        let root = normalized(&policy.root);
        if policy.max_file_size_bytes == 0
            || !seen.insert(root.clone())
            || !config
                .roots
                .iter()
                .chain(&config.code_roots)
                .any(|p| normalized(p) == root)
            || policy.exclusions.len() > 512
        {
            return Err(bad());
        }
        exclusions += policy.exclusions.len();
        if exclusions > 512 {
            return Err(bad());
        }
        for pattern in &mut policy.exclusions {
            let value = pattern.trim().replace('\\', "/");
            if value.is_empty()
                || value.len() > 1024
                || value.contains('\0')
                || value.starts_with('/')
                || value.as_bytes().get(1) == Some(&b':')
                || value.split('/').any(|part| part == "..")
            {
                return Err(bad());
            }
            *pattern = value.to_ascii_lowercase();
        }
    }
    Ok(())
}

fn selected_root<'a>(config: &'a IndexingConfig, path: &str, code: bool) -> Option<&'a str> {
    let roots = if code {
        &config.code_roots
    } else {
        &config.roots
    };
    roots
        .iter()
        .filter(|root| contains(root, path))
        .max_by_key(|root| normalized(root).len())
        .map(String::as_str)
}

fn rule<'a>(config: &'a IndexingConfig, root: &str) -> Option<&'a RootPolicy> {
    let root = normalized(root);
    config
        .root_policies
        .iter()
        .find(|policy| normalized(&policy.root) == root)
}

pub fn relative_allowed(
    config: &IndexingConfig,
    root: &str,
    path: &str,
    filter_hidden: bool,
) -> bool {
    let Some(policy) = rule(config, root) else {
        return true;
    };
    let root = normalized(root);
    let path = normalized(path);
    let Some(relative) = path.strip_prefix(&root) else {
        return false;
    };
    let relative = relative.trim_start_matches('\\').replace('\\', "/");
    if filter_hidden
        && !policy.include_hidden
        && relative
            .split('/')
            .any(|part| part.starts_with('.') && part.len() > 1)
    {
        return false;
    }
    !policy.exclusions.iter().any(|pattern| {
        if let Some(suffix) = pattern.strip_prefix('*') {
            relative.ends_with(suffix)
        } else {
            relative == *pattern
                || relative.starts_with(&format!("{pattern}/"))
                || relative.split('/').any(|part| part == pattern)
        }
    })
}

pub fn include_hidden(config: &IndexingConfig, path: &str, code: bool) -> bool {
    selected_root(config, path, code)
        .and_then(|root| rule(config, root))
        .is_some_and(|policy| policy.include_hidden)
}

pub fn metadata_allowed(config: &IndexingConfig, path: &Path, code: bool) -> bool {
    let text = path.to_string_lossy();
    let Some(root) = selected_root(config, &text, code) else {
        return false;
    };
    let Some(policy) = rule(config, root) else {
        return true;
    };
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return false;
    };
    if metadata.is_file() && metadata.len() > policy.max_file_size_bytes {
        return false;
    }
    let root = normalized(root);
    for ancestor in path.ancestors() {
        if normalized(&ancestor.to_string_lossy()) == root {
            return true;
        }
        let Ok(metadata) = std::fs::symlink_metadata(ancestor) else {
            return false;
        };
        let flags = metadata.file_attributes();
        if flags & 0x4 != 0 || (!policy.include_hidden && flags & 0x2 != 0) {
            return false;
        }
    }
    false
}
