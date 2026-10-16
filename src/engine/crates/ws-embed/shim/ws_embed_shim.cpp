// Flat C shim over llama.cpp / ggml / mtmd.
//
// llama_model_params and llama_context_params are passed BY VALUE in the
// upstream C API and are large, version-dependent structs. Keeping them on the
// C++ side means the Rust FFI only sees pointers and scalars, so a pinned
// llama.cpp bump changes this file, not the Rust declarations.

#include "llama.h"
#include "ggml-backend.h"
#include "ggml.h"
#include "mtmd.h"

#include <cstdint>
#include <cstring>
#include <cmath>
#include <memory>

extern "C" {

// ---- backend registry ------------------------------------------------------

// Loads every backend DLL found next to the executable (GGML_BACKEND_DL).
void wse_backend_load_all(void) {
    ggml_backend_load_all();
}

// Loads backend DLLs from an explicit directory.
void wse_backend_load_all_from_path(const char * dir) {
    ggml_backend_load_all_from_path(dir);
}

size_t wse_dev_count(void) {
    return ggml_backend_dev_count();
}

// Returns the device at index, or NULL.
ggml_backend_dev_t wse_dev_get(size_t index) {
    return ggml_backend_dev_get(index);
}

// Writes the registry name ("CUDA"/"Vulkan"/"CPU"/...) into buf (NUL-terminated).
size_t wse_dev_backend_name(ggml_backend_dev_t dev, char * buf, size_t cap) {
    if (!dev || !buf || cap == 0) {
        return 0;
    }
    const char * name = ggml_backend_reg_name(ggml_backend_dev_backend_reg(dev));
    if (!name) {
        name = "";
    }
    size_t n = std::strlen(name);
    if (n >= cap) {
        n = cap - 1;
    }
    std::memcpy(buf, name, n);
    buf[n] = '\0';
    return n;
}

// 0 = CPU, 1 = GPU, 2 = IGPU, 3 = ACCEL, 4 = META, -1 = invalid
int wse_dev_type(ggml_backend_dev_t dev) {
    if (!dev) {
        return -1;
    }
    return (int) ggml_backend_dev_type(dev);
}

void wse_dev_memory(ggml_backend_dev_t dev, size_t * free_bytes, size_t * total_bytes) {
    ggml_backend_dev_memory(dev, free_bytes, total_bytes);
}

// Writes the device name into buf (NUL-terminated); returns bytes written.
size_t wse_dev_name(ggml_backend_dev_t dev, char * buf, size_t cap) {
    if (!dev || !buf || cap == 0) {
        return 0;
    }
    const char * name = ggml_backend_dev_name(dev);
    if (!name) {
        name = "";
    }
    size_t n = std::strlen(name);
    if (n >= cap) {
        n = cap - 1;
    }
    std::memcpy(buf, name, n);
    buf[n] = '\0';
    return n;
}

// ---- logging ----------------------------------------------------------------

// Installs a callback. Pass NULL to restore the default stderr output.
void wse_log_set(ggml_log_callback cb, void * user_data) {
    llama_log_set(cb, user_data);
}

// ---- model ------------------------------------------------------------------

// Loads a GGUF. backend_dev may be NULL (= all devices). n_gpu_layers < 0 means
// all layers; 0 forces CPU. Returns NULL on failure.
struct llama_model * wse_model_load(const char * path, ggml_backend_dev_t backend_dev, int32_t n_gpu_layers) {
    llama_model_params params = llama_model_default_params();
    params.n_gpu_layers = n_gpu_layers;
    ggml_backend_dev_t devices[2] = { backend_dev, nullptr };
    if (backend_dev) {
        params.devices = devices;
    }
    return llama_model_load_from_file(path, params);
}

void wse_model_free(struct llama_model * model) {
    llama_model_free(model);
}

// Pooled OUTPUT embedding dimension. Not llama_model_n_embd, which is the input
// width: EmbeddingGemma 2 has input 512 but output 768 (embedding_length_out).
int32_t wse_model_n_embd(const struct llama_model * model) {
    return llama_model_n_embd_out(model);
}

// ---- context ----------------------------------------------------------------

// Maximum sequences per decode. The context must be created with this capacity:
// llama_batch rejects any seq_id >= n_seq_max (default 1), which made every
// multi-text batch fail with -1. Callers chunk to this size.
#define WSE_N_SEQ_MAX 32

// Creates an embedding-mode context with reference mean pooling.
struct llama_context * wse_context_new(struct llama_model * model, uint32_t n_ctx, uint32_t n_batch, int32_t n_threads) {
    llama_context_params params = llama_context_default_params();
    params.n_ctx = n_ctx;
    params.n_batch = n_batch;
    params.n_ubatch = n_batch;
    params.n_seq_max = WSE_N_SEQ_MAX;
    params.kv_unified = true;
    params.n_threads = n_threads;
    params.n_threads_batch = n_threads;
    params.embeddings = true;
    params.pooling_type = LLAMA_POOLING_TYPE_MEAN;
    params.no_perf = true;
    return llama_init_from_model(model, params);
}

// Sequences per decode the context supports (chunk size for callers).
int32_t wse_context_max_seqs(void) {
    return WSE_N_SEQ_MAX;
}

void wse_context_free(struct llama_context * ctx) {
    llama_free(ctx);
}

int32_t wse_context_pooling_type(const struct llama_context * ctx) {
    return (int32_t) llama_pooling_type(ctx);
}

// ---- tokenize / embed --------------------------------------------------------

// Tokenizes text with special tokens (BOS/EOS as the model defines). Returns the
// token count, or a negative value holding the required size (-n) when cap is
// too small, matching llama_tokenize semantics.
int32_t wse_tokenize(const struct llama_model * model, const char * text, int32_t text_len, int32_t * out, int32_t cap) {
    const struct llama_vocab * vocab = llama_model_get_vocab(model);
    return llama_tokenize(vocab, text, text_len, (llama_token *) out, cap, true, true);
}

// Runs one embedding pass over a batch of sequences and copies each pooled
// sequence embedding into out (row-major, n_seqs x n_embd).
//
// tokens: all sequences concatenated; seq_lens[i] = token count of sequence i.
// Returns 0 on success, non-zero llama_decode code on failure, -1 on bad args.
int32_t wse_embed_batch(struct llama_context * ctx, const int32_t * tokens, const int32_t * seq_lens, int32_t n_seqs, int32_t n_embd, float * out) {
    if (!ctx || !tokens || !seq_lens || n_seqs <= 0 || n_seqs > WSE_N_SEQ_MAX || n_embd <= 0 || !out) {
        return -1;
    }
    int32_t n_total = 0;
    for (int32_t s = 0; s < n_seqs; ++s) {
        if (seq_lens[s] <= 0) {
            return -1;
        }
        n_total += seq_lens[s];
    }
    const uint32_t max_batch = llama_n_batch(ctx);
    const uint32_t max_ubatch = llama_n_ubatch(ctx);
    if ((max_batch > 0 && (uint32_t) n_total > max_batch) ||
        (max_ubatch > 0 && (uint32_t) n_total > max_ubatch)) {
        return -1;
    }

    llama_memory_clear(llama_get_memory(ctx), true);

    llama_batch batch = llama_batch_init(n_total, 0, n_seqs);
    int32_t idx = 0;
    for (int32_t s = 0; s < n_seqs; ++s) {
        for (int32_t t = 0; t < seq_lens[s]; ++t) {
            batch.token[idx] = tokens[idx];
            batch.pos[idx] = t;
            batch.n_seq_id[idx] = 1;
            batch.seq_id[idx][0] = s;
            batch.logits[idx] = 1;
            ++idx;
        }
    }
    batch.n_tokens = n_total;

    int32_t rc = llama_decode(ctx, batch);
    if (rc == 0) {
        for (int32_t s = 0; s < n_seqs; ++s) {
            float * e = llama_get_embeddings_seq(ctx, s);
            if (!e) {
                rc = -2;
                break;
            }
            std::memcpy(out + (size_t) s * n_embd, e, sizeof(float) * n_embd);
        }
    }
    llama_batch_free(batch);
    return rc;
}

// ---- multimodal (mtmd) -------------------------------------------------------

// Opens the mmproj. use_gpu=false forces the projector onto CPU. Returns NULL on
// failure. Inputs arrive at the reference's final pixel/token budget.
mtmd_context * wse_mtmd_init(const char * mmproj_path, const struct llama_model * text_model, bool use_gpu, int32_t n_threads) {
    mtmd_context_params p = mtmd_context_params_default();
    p.use_gpu = use_gpu;
    p.n_threads = n_threads;
    p.warmup = false;
    p.image_min_tokens = 1;
    p.image_max_tokens = 1120;
    return mtmd_init_from_file(mmproj_path, text_model, p);
}

void wse_mtmd_free(mtmd_context * ctx) {
    mtmd_free(ctx);
}

int32_t wse_mtmd_supports_vision(const mtmd_context * ctx) {
    return mtmd_support_vision(ctx) ? 1 : 0;
}

int32_t wse_mtmd_supports_audio(const mtmd_context * ctx) {
    return mtmd_support_audio(ctx) ? 1 : 0;
}

// Bidirectional pooling needs BOS/media/EOS together, rather than incremental DECODE.
static int32_t wse_embed_media(mtmd_context * mctx, llama_context * lctx,
                               const mtmd_bitmap * bmp, int32_t n_embd, float * out) {
    const llama_model * model = llama_get_model(lctx);
    const int32_t stride = llama_model_n_embd_inp(model);
    if (stride <= 0 || stride > 16384 || n_embd != llama_model_n_embd_out(model)) return -1;
    const char * marker = mtmd_default_marker();
    if (!marker) return -2;
    std::unique_ptr<mtmd_input_chunks, decltype(&mtmd_input_chunks_free)> chunks(
        mtmd_input_chunks_init(), mtmd_input_chunks_free);
    std::unique_ptr<llama_batch_ext, decltype(&llama_batch_ext_free)> batch(
        llama_batch_ext_init(lctx), llama_batch_ext_free);
    if (!chunks || !batch) return -3;
    mtmd_input_text text{marker, std::strlen(marker), true, true};
    const mtmd_bitmap * bitmaps[] = {bmp};
    if (mtmd_tokenize(mctx, chunks.get(), &text, bitmaps, 1) != 0) return -4;
    const size_t count = mtmd_input_chunks_size(chunks.get());
    if (count == 0 || count > 2048) return -4;
    size_t total = 0;
    for (size_t i = 0; i < count; ++i) {
        const size_t n = mtmd_input_chunk_get_n_tokens(mtmd_input_chunks_get(chunks.get(), i));
        if (n > 2048 - total) return -5;
        total += n;
    }
    if (total == 0 || total > llama_n_batch(lctx) || total > llama_n_ubatch(lctx)) return -5;
    llama_pos position = 0;
    for (size_t i = 0; i < count; ++i) {
        const mtmd_input_chunk * chunk = mtmd_input_chunks_get(chunks.get(), i);
        const bool is_text = mtmd_input_chunk_get_type(chunk) == MTMD_INPUT_CHUNK_TYPE_TEXT;
        size_t n = mtmd_input_chunk_get_n_tokens(chunk);
        const llama_token * tokens = nullptr;
        float * rows = nullptr;
        if (is_text) {
            tokens = mtmd_input_chunk_get_tokens_text(chunk, &n);
            if (n != mtmd_input_chunk_get_n_tokens(chunk) || (n && !tokens)) return -6;
        } else {
            if (mtmd_encode_chunk(mctx, chunk) != 0) return -6;
            rows = mtmd_get_output_embd(mctx);
            if (n && !rows) return -6;
        }
        for (size_t row = 0; row < n; ++row) {
            const int32_t index = is_text
                ? llama_batch_ext_add_token(batch.get(), 0, tokens[row])
                : llama_batch_ext_add_embd(batch.get(), 0, {rows + row * stride, 1, (size_t) stride});
            const llama_pos pos[] = {position, 0, 0, 0};
            if (index < 0 || !llama_batch_ext_set_pos(batch.get(), index, pos) ||
                !llama_batch_ext_set_output_embd(batch.get(), index, true)) return -7;
            ++position;
        }
    }
    llama_memory_clear(llama_get_memory(lctx), true);
    const int32_t rc = llama_process(lctx, LLAMA_PROCESS_TYPE_ENCODE, batch.get());
    if (rc != 0) return rc;
    const float * pooled = llama_get_embeddings_seq(lctx, 0);
    if (!pooled) return -8;
    std::memcpy(out, pooled, sizeof(float) * (size_t) n_embd);
    return 0;
}

int32_t wse_mtmd_embed_image(
    mtmd_context * mctx,
    struct llama_context * lctx,
    const unsigned char * rgb,
    uint32_t width,
    uint32_t height,
    int32_t n_embd,
    float * out
) {
    if (!mctx || !lctx || !rgb || width == 0 || height == 0 || n_embd <= 0 || !out) {
        return -1;
    }
    if (!mtmd_support_vision(mctx)) {
        return -2;
    }
    if (width > 13440 || height > 13440 || (uint64_t)width * height > 280 * 48 * 48) return -1;
    try {
        std::unique_ptr<mtmd_bitmap, decltype(&mtmd_bitmap_free)> bmp(
            mtmd_bitmap_init(width, height, rgb), mtmd_bitmap_free);
        return bmp ? wse_embed_media(mctx, lctx, bmp.get(), n_embd, out) : -3;
    } catch (...) { return -9; }
}

int32_t wse_mtmd_embed_audio(mtmd_context * mctx, llama_context * lctx,
                            const float * pcm, size_t n_samples, int32_t n_embd, float * out) {
    if (!mctx || !lctx || !pcm || !out || n_embd <= 0 || n_samples == 0 || n_samples > 480000) return -1;
    if (!mtmd_support_audio(mctx)) return -2;
    for (size_t i = 0; i < n_samples; ++i) if (!std::isfinite(pcm[i])) return -1;
    try {
        std::unique_ptr<mtmd_bitmap, decltype(&mtmd_bitmap_free)> bmp(
            mtmd_bitmap_init_from_audio(n_samples, pcm), mtmd_bitmap_free);
        return bmp ? wse_embed_media(mctx, lctx, bmp.get(), n_embd, out) : -3;
    } catch (...) { return -9; }
}

} // extern "C"
