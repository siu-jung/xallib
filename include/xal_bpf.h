#ifndef XAL_BPF_H
#define XAL_BPF_H

#define XAL_BPF_RUNNING	1

struct xal_bpf {
	struct xal_bpf_ctx ctx;
	struct xal_bpf_stat stat;
	struct ring_buffer *rb;
	struct xal_events_bpf *skel;
	pthread_t bpf_poll_thread_id;
	int flag;
};

int
xal_be_fiemap_bpf_rb_init(struct xal_bpf *bpf);

int
xal_be_fiemap_bpf_init(struct xal_bpf *bpf);

void
xal_be_fiemap_bpf_close(struct xal_bpf *bpf);
#endif
