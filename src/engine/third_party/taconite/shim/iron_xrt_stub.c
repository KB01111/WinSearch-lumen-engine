/* SPDX-License-Identifier: Apache-2.0
 *
 * Stand-in for iron_xrt_shim.cpp when the build found no XRT SDK: every
 * entry point fails cleanly, so the Rust side reports "no NPU runtime" and
 * callers fall back to another backend instead of failing to link or start.
 */
#include "iron_xrt_shim.h"

#include <string.h>

static void stub_err(char *err, size_t err_len) {
    static const char msg[] =
        "built without the XRT SDK (set XRT_ROOT to an XRT install and rebuild for NPU support)";
    if (err && err_len) {
        size_t n = sizeof(msg) - 1 < err_len - 1 ? sizeof(msg) - 1 : err_len - 1;
        memcpy(err, msg, n);
        err[n] = '\0';
    }
}

iron_session *iron_open(int device_index, char *err, size_t err_len) {
    (void)device_index;
    stub_err(err, err_len);
    return NULL;
}
void iron_close(iron_session *s) { (void)s; }

iron_kernel *iron_kernel_load(iron_session *s, const char *xclbin_path, const char *insts_path,
                              const char *kernel_name, uint32_t gops, char *err, size_t err_len) {
    (void)s; (void)xclbin_path; (void)insts_path; (void)kernel_name; (void)gops;
    stub_err(err, err_len);
    return NULL;
}
void iron_kernel_free(iron_kernel *k) { (void)k; }

iron_buffer *iron_buffer_alloc(iron_session *s, size_t bytes, char *err, size_t err_len) {
    (void)s; (void)bytes;
    stub_err(err, err_len);
    return NULL;
}
void *iron_buffer_map(iron_buffer *b) { (void)b; return NULL; }
size_t iron_buffer_size(iron_buffer *b) { (void)b; return 0; }
int iron_buffer_sync_to_device(iron_buffer *b, char *err, size_t err_len) {
    (void)b;
    stub_err(err, err_len);
    return 1;
}
int iron_buffer_sync_from_device(iron_buffer *b, char *err, size_t err_len) {
    (void)b;
    stub_err(err, err_len);
    return 1;
}
void iron_buffer_free(iron_buffer *b) { (void)b; }
iron_buffer *iron_buffer_sub(iron_buffer *parent, size_t offset, size_t bytes, char *err, size_t err_len) {
    (void)parent; (void)offset; (void)bytes;
    stub_err(err, err_len);
    return NULL;
}

int iron_kernel_run(iron_kernel *k, iron_buffer **bufs, size_t n, uint64_t *elapsed_ns, char *err,
                    size_t err_len) {
    (void)k; (void)bufs; (void)n; (void)elapsed_ns;
    stub_err(err, err_len);
    return 1;
}
int iron_kernel_set_insts_words(iron_kernel *k, const uint32_t *idx, const uint32_t *vals, size_t n,
                                char *err, size_t err_len) {
    (void)k; (void)idx; (void)vals; (void)n;
    stub_err(err, err_len);
    return 1;
}
iron_run *iron_kernel_start(iron_kernel *k, iron_buffer **bufs, size_t n, char *err, size_t err_len) {
    (void)k; (void)bufs; (void)n;
    stub_err(err, err_len);
    return NULL;
}
int iron_run_wait(iron_run *r, uint64_t *elapsed_ns, char *err, size_t err_len) {
    (void)r; (void)elapsed_ns;
    stub_err(err, err_len);
    return 1;
}
