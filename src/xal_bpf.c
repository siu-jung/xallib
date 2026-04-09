#include <errno.h>
#include <fcntl.h>
#include <khash.h>
#include <libxal.h>
#include <linux/fs.h>
#include <poll.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <bpf/bpf.h>
#include <bpf/libbpf.h>

#include <xal.h>
#include <xal_be_fiemap.h>
#include <xal_odf.h>
#include <xal_bpf_events.h>
#include <xal_bpf.h>
#include <xal_events.skel.h>

static int
handle_event(void *__ctx, void *__data, size_t len)
{
	struct xal_bpf_ctx *ctx = __ctx;
	const struct xfs_extent_event *e = __data;

	if (len < sizeof(*e)) {
		XAL_DEBUG("FAILED: size of event too small; len(%lu) sizeof(e)(%lu)", len, sizeof(*e));
		return -EINVAL;
	}

	if (e->dev_major != ctx->dev_major || e->dev_minor != ctx->dev_minor) {
		XAL_DEBUG("INFO: maj,min mismatch, ignoring bpf event ctx(%d,%d) e(%d,%d)",
				ctx->dev_major, ctx->dev_minor, e->dev_major, e->dev_minor);
		return 0;
	}

	XAL_DEBUG("NOTICE: bpf event(%u) ino(%lu) startoff(%lu) startblock(%lu) blockcount(%lu) state(%u) bmap_state(%d) fsblock(%u)",
			e->type, e->ino, e->startoff, e->startblock, e->blockcount, e->state, e->bmap_state, e->fs_block_size);
	// update extent info
	switch (e->type) {
	case XFS_EVENT_MAP_ALLOC:
		// new extent mapping added
		break;
	case XFS_EVENT_BUNMAP:
		// extent removed
		break;
	default:
		break;
	}

	return 0;
}

int
xal_be_fiemap_bpf_rb_init(struct xal_bpf *bpf)
{
	struct ring_buffer *rb;
	int err;

	if (!bpf) {
		XAL_DEBUG("FAILED: No xal_bpf given");
		return -EINVAL;
	}

	// If already exist, free first for clean init
	if (bpf->rb) {
		ring_buffer__free(bpf->rb);
	}

	rb = ring_buffer__new(bpf_map__fd(bpf->skel->maps.events), handle_event, &bpf->ctx, NULL);
	if (!rb) {
		err = -errno;
		XAL_DEBUG("FAILD: ring_buffer__new(); err(%d)", err);
		goto out;
	}

	bpf->rb = rb;

	XAL_DEBUG("INFO: bpf ring buffer initialized");
	return 0;

out:
	ring_buffer__free(rb);
	return err;
}

static int
configure_xal_bpf(struct xal_events_bpf *skel, const struct xal_bpf_ctx *ctx)
{
	/*
	struct monitor_cfg cfg = {
		.dev_major = bpf->dev_major;
		.dev_minor = bpf->dev_minor;
		.fs_block_size = bpf->fs_block_size;
	};
	*/
	uint32_t key = 0;
	int fd = bpf_map__fd(skel->maps.ctx_map);

	if (bpf_map_update_elem(fd, &key, ctx, BPF_ANY) < 0) {
		return -errno;
	}
	return 0;
}

int
xal_be_fiemap_bpf_init(struct xal_bpf *bpf)
{
	struct xal_events_bpf *skel = NULL;
//	struct ring_buffer *rb = NULL;
	int err;

	if (!bpf) {
		XAL_DEBUG("FAILED: No xal_bpf given");
		return -EINVAL;
	}

	libbpf_set_strict_mode(LIBBPF_STRICT_ALL);
	libbpf_set_print(NULL);

	skel = xal_events_bpf__open();
	if (!skel) {
		err = -ENOMEM;
		XAL_DEBUG("FAILED: xal_events_bpf__open()");
		goto out;
	}

	err = xal_events_bpf__load(skel);
	if (err) {
		XAL_DEBUG("FAILED: xal_events_bpf__load(); err(%d)", err);
		goto out;
	}

	err = configure_xal_bpf(skel, &bpf->ctx);
	if (err < 0) {
		XAL_DEBUG("FAILED: configure_xal_bpf(); err(%d)", err);
		goto out;
	}

	err = xal_events_bpf__attach(skel);
	if (err) {
		XAL_DEBUG("FAILED: xal_events_bpf__attach(); err(%d)", err);
		goto out;
	}

	/*
	rb = ring_buffer__new(bpf_map__fd(skel->maps.events), handle_event, bpf, NULL);
	if (!rb) {
		err = -errno;
		XAL_DEBUG("FAILED: ring_buffer__new(); err(%d)", err);
		goto out;
	}
	*/
	bpf->skel = skel;

	return 0;
out:
	xal_events_bpf__destroy(skel);
	return err;
}

void
xal_be_fiemap_bpf_close(struct xal_bpf *bpf)
{
	if (!bpf) {
		XAL_DEBUG("SKIPPED: No xal_bpf given");
		return;
	}

	if (bpf->flag & XAL_BPF_RUNNING) {
		pthread_cancel(bpf->bpf_poll_thread_id);
		XAL_DEBUG("INFO: xal_be_fiemap_bpf_close() stopping bpf poll thread");
	}

	// maybe ring_buffer__free(rb);
	if (bpf->rb) {
		ring_buffer__free(bpf->rb);
	}

	if (bpf->skel) {
		xal_events_bpf__destroy(bpf->skel);
	}

	free(bpf);

	return;
}

static int
read_stats(struct xal_events_bpf *skel, struct xal_bpf_stat *out)
{
	uint32_t key = 0;
	int fd = bpf_map__fd(skel->maps.stat_map);

	memset(out, 0, sizeof(*out));
	if (bpf_map_lookup_elem(fd, &key, out) < 0) {
		return -errno;
	}
	return 0;
}

static void *
background_bpf_poll(void *arg)
{
	struct xal *xal = arg;
	struct xal_be_fiemap *be = (struct xal_be_fiemap *)&xal->be;
	struct xal_bpf *bpf;
	struct xal_bpf_stat st;
	int err = 0;

	XAL_DEBUG("INFO: starting background bpf poll thread");

	if (!be->bpf) {
		XAL_DEBUG("FAILED: bpf not initialized, exit thread");
		goto exit_thread;
	}

	bpf = be->bpf;
	if (!bpf->rb) {
		XAL_DEBUG("FAILED: xal bpf ring buffer not initialized");
		goto exit_thread;
	}
	bpf->flag |= XAL_BPF_RUNNING;

	err = pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	if (err) {
		XAL_DEBUG("FAILED: pthread_setcancelstate(), exit bpf thread; err(%d)", err);
		goto exit_flag;
	}

	while (1) {
		// do the polling
		//XAL_DEBUG("INFO: bpf polling ring buffer...");
		err = ring_buffer__poll(bpf->rb, 200);
		if (err < 0 && err != -EINTR) {
			XAL_DEBUG("FAILED: ring_buffer__poll(); err(%d)", err);
			goto exit_flag;
		}
		err = ring_buffer__consume(bpf->rb);
		if (!err) {
			//XAL_DEBUG("INFO: no bpf events... polling again...");
			continue;
		}
		err = 0;
		// read the event
		if (read_stats(bpf->skel, &st) == 0) {
			// check stats for lost events
		}
	}

exit_flag:
	bpf->flag &= ~XAL_BPF_RUNNING;
exit_thread:
	XAL_DEBUG("INFO: unlocked xal lock");
	pthread_exit((void *)(intptr_t)err);
}

int
xal_bpf_start_poll_thread(struct xal *xal)
{
	struct xal_backend_base *base;
	struct xal_be_fiemap *be;
	struct xal_bpf *bpf;
	int err;

	if (!xal) {
		XAL_DEBUG("FAILED: no xal given");
		return -EINVAL;
	}

	base = (struct xal_backend_base *)&xal->be;
	if (base->type != XAL_BACKEND_FIEMAP) {
		XAL_DEBUG("FAILED: Invalid backend type(%d)", base->type);
		return -EINVAL;
	}

	be = (struct xal_be_fiemap *)base;
	if (!be->bpf) {
		XAL_DEBUG("FAILED: xal opened without bpf");
		return -EINVAL;
	}

	bpf = be->bpf;
	if (!bpf->skel) {
		XAL_DEBUG("FAILED: xal_events_bpf skel not initialized");
		return -EINVAL;
	}

	if (!bpf->rb) {
		XAL_DEBUG("FAILED: xal bpf ring buffer not initialized");
		return -EINVAL;
	}

	if (bpf->flag & XAL_BPF_RUNNING) {
		XAL_DEBUG("SKIPPED: bpf thread already running");
		return 0;
	}

	if (xal->root_idx == XAL_POOL_IDX_NONE) {
		XAL_DEBUG("FAILED: Missing call to xal_index()");
		return -EINVAL;
	}

	err = pthread_create(&bpf->bpf_poll_thread_id, NULL, &background_bpf_poll, xal);
	if (err) {
		XAL_DEBUG("FAILED: pthread_create(); err(%d)", err);
		return -err;
	}

	return 0;
}

int
xal_bpf_stop_poll_thread(struct xal *xal)
{
	struct xal_backend_base *base;
	struct xal_be_fiemap *be;
	struct xal_bpf *bpf;
	int err;

	if (!xal) {
		XAL_DEBUG("FAILED: no xal given");
		return -EINVAL;
	}

	base = (struct xal_backend_base *)&xal->be;
	if (base->type != XAL_BACKEND_FIEMAP) {
		XAL_DEBUG("FAILED: Invalid backend type(%d)", base->type);
		return -EINVAL;
	}

	be = (struct xal_be_fiemap *)base;
	if (!be->bpf) {
		XAL_DEBUG("FAILED: xal opened without bpf");
		return -EINVAL;
	}

	bpf = be->bpf;
	if (!bpf->skel) {
		XAL_DEBUG("FAILED: xal_events_bpf skel not initialized");
		return -EINVAL;
	}

	if (bpf->flag & ~XAL_BPF_RUNNING) {
		XAL_DEBUG("FAILED: bpf thread is not running");
		return -EINVAL;
	}

	pthread_cancel(bpf->bpf_poll_thread_id);
	err = pthread_cancel(bpf->bpf_poll_thread_id);
	if (err) {
		XAL_DEBUG("FAILED: pthread_cancel(); err(%d)", err);
		return -err;
	}
	XAL_DEBUG("INFO: xal_bpf_stop_poll_thread() stopping bpf poll thread");

	bpf->flag &= ~XAL_BPF_RUNNING;

	return 0;
}
