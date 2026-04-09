#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>
#include <xal_bpf_events.h>

char LICENSE[] SEC("license") = "GPL";

struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 1 << 24);
} events SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, struct xal_bpf_stat);
} stat_map SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, struct xal_bpf_ctx);
} ctx_map SEC(".maps");

static __always_inline void
note_lost_event(void)
{
	uint32_t key = 0;
	struct xal_bpf_stat *st = bpf_map_lookup_elem(&stat_map, &key);
	if (st) {
		__sync_fetch_and_add(&st->lost_events, 1);
	}
}

static __always_inline void
note_ignored_event(void)
{
	uint32_t key = 0;
	struct xal_bpf_stat *st = bpf_map_lookup_elem(&stat_map, &key);
	if (st) {
		__sync_fetch_and_add(&st->ignored_events, 1);
	}
}

static __always_inline uint32_t
dev_major_from_dev(uint32_t dev)
{
	return dev >> 20;
}

static __always_inline uint32_t
dev_minor_from_dev(uint32_t dev)
{
	return dev & ((1U << 20) - 1);
}

static __always_inline struct xal_bpf_ctx
*get_ctx(void)
{
	uint32_t key = 0;
	return bpf_map_lookup_elem(&ctx_map, &key);
}

static __always_inline bool
match_fs(uint32_t dev)
{
	struct xal_bpf_ctx *ctx = get_ctx();
	if (!ctx) {
		return false;
	}

	if (ctx->dev_major != dev_major_from_dev(dev) ||
		ctx->dev_minor != dev_minor_from_dev(dev)) {
		note_ignored_event();
		return false;
	}

	return true;
}

static __always_inline int
emit_event(uint32_t type, uint32_t dev, uint64_t ino, uint64_t startoff, uint64_t startblock,
		uint64_t blockcount, uint32_t state, int32_t bmap_state)
{
	struct xfs_extent_event *e;
	struct xal_bpf_ctx *ctx;
	uint64_t id;

	if (!match_fs(dev)) {
		return 0;
	}

	ctx = get_ctx();
	if (!ctx) {
		return 0;
	}

	id = bpf_get_current_pid_tgid();
	e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
	if (!e) {
		note_lost_event();
		return 0;
	}

	e->ts_ns = bpf_ktime_get_ns();
	e->pid = (uint32_t)id;
	e->tgid = (uint32_t)(id >> 32);
	e->cpu = bpf_get_smp_processor_id();

	e->dev_major = ctx->dev_major;
	e->dev_minor = ctx->dev_minor;
	e->ino = ino;

	e->type = type;
	e->fs_block_size = ctx->fs_block_size;

	e->startoff = startoff;
	e->startblock = startblock;
	e->blockcount = blockcount;
	e->state = state;
	e->bmap_state = bmap_state;

	bpf_ringbuf_submit(e, 0);
	return 0;
}

/*
 * XFS tracepoints where we place BPF hooks.
 * ABI may change depending on target kernel.
 * Update if mismatch.
 */
SEC("tracepoint/xfs/xfs_map_blocks_alloc")
int
tp_xfs_map_blocks_alloc(struct xfs_imap_ctx *ctx)
{
	if (ctx->whichfork != 0) {
		return 0;
	}

	return emit_event(XFS_EVENT_MAP_ALLOC, ctx->dev, ctx->ino,
			ctx->startoff, ctx->startblock, ctx->blockcount, 0, 0);
}
/*
SEC("tracepoint/xfs/xfs_map_blocks_found")
int
tp_xfs_map_blocks_found(struct xfs_imap_ctx *ctx)
{
	if (ctx->whichfork != 0) {
		return 0;
	}

	return emit_event(XFS_EVENT_MAP_FOUND, ctx->dev, ctx->ino,
			ctx->startoff, ctx->startblock, ctx->blockcount, 0);
}
*/
SEC("tracepoint/xfs/xfs_bmap_post_update")
int
tp_xfs_bmap_post_update(struct xfs_bmap_ctx *ctx)
{
	return emit_event(XFS_EVENT_BMAP_POST_UPDATE, ctx->dev, ctx->ino,
			ctx->startoff, ctx->startblock, ctx->blockcount, ctx->state, ctx->bmap_state);
}

SEC("tracepoint/xfs/xfs_bunmap")
int
tp_xfs_bunmap(struct xfs_bunmap_ctx *ctx)
{
	return emit_event(XFS_EVENT_BUNMAP, ctx->dev, ctx->ino,
			ctx->fileoff, -1, ctx->len, 0, 0);
}
