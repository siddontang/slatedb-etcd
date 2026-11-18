/* Compare one durable WAL batch at queue depth one on a Linux file system. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <liburing.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static void fail(const char *operation, int error)
{
    fprintf(stderr, "%s: %s\n", operation, strerror(error));
    exit(1);
}

static uint64_t now_ns(void)
{
    struct timespec time;
    if (clock_gettime(CLOCK_MONOTONIC, &time) < 0)
        fail("clock_gettime", errno);
    return (uint64_t)time.tv_sec * 1000000000 + time.tv_nsec;
}

static void linked_write(struct io_uring *ring, int fd, char *data, unsigned size, off_t pos)
{
    struct io_uring_sqe *write = io_uring_get_sqe(ring);
    struct io_uring_sqe *sync = io_uring_get_sqe(ring);
    if (!write || !sync)
        fail("submission queue", ENOSPC);
    io_uring_prep_write(write, fd, data, size, pos);
    io_uring_sqe_set_flags(write, IOSQE_IO_LINK);
    io_uring_sqe_set_data64(write, 1);
    io_uring_prep_fsync(sync, fd, IORING_FSYNC_DATASYNC);
    io_uring_sqe_set_data64(sync, 2);
    int result = io_uring_submit_and_wait(ring, 2);
    if (result != 2)
        fail("submit and wait", result < 0 ? -result : EIO);
    unsigned seen = 0;
    for (unsigned i = 0; i < 2; i++) {
        struct io_uring_cqe *cqe;
        result = io_uring_wait_cqe(ring, &cqe);
        if (result < 0)
            fail("wait for completion", -result);
        uint64_t id = io_uring_cqe_get_data64(cqe);
        if (id != 1 && id != 2)
            fail("completion ID", EIO);
        if (cqe->res != (id == 1 ? (int)size : 0))
            fail("write or sync completion", cqe->res < 0 ? -cqe->res : EIO);
        seen |= (unsigned)id;
        io_uring_cqe_seen(ring, cqe);
    }
    if (seen != 3)
        fail("missing completion", EIO);
}

static int compare(const void *left, const void *right)
{
    uint64_t a = *(const uint64_t *)left, b = *(const uint64_t *)right;
    return (a > b) - (a < b);
}

int main(int argc, char **argv)
{
    if (argc != 3 || (strcmp(argv[1], "sync") && strcmp(argv[1], "ring")) ||
        (strcmp(argv[2], "1024") && strcmp(argv[2], "4096"))) {
        fprintf(stderr, "Usage: wal-sync {sync|ring} {1024|4096}\n");
        return 2;
    }
    const int use_ring = !strcmp(argv[1], "ring");
    const unsigned size = (unsigned)atoi(argv[2]);
    enum { warmup = 100, batches = 3000 };
    struct io_uring ring;
    if (use_ring) {
        int err = io_uring_queue_init(4, &ring, 0);
        if (err < 0)
            fail("io_uring_queue_init", -err);
    }
    char path[] = "wal-sync-XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0 || unlink(path) < 0)
        fail("temporary log", errno);
    int err = posix_fallocate(fd, 0, (off_t)(batches + warmup) * size);
    if (err || fdatasync(fd) < 0)
        fail("prepare log", err ? err : errno);
    uint64_t samples[batches], elapsed = 0;
    char data[4096], actual[4096];
    for (int i = 0; i < batches + warmup; i++) {
        memset(data, i % 251, size);
        off_t pos = (off_t)i * size;
        uint64_t start = now_ns();
        if (use_ring) {
            linked_write(&ring, fd, data, size, pos);
        } else {
            ssize_t count = pwrite(fd, data, size, pos);
            if (count != (ssize_t)size || fdatasync(fd) < 0)
                fail("write and sync", count >= 0 && count != (ssize_t)size ? EIO : errno);
        }
        if (i >= warmup) {
            samples[i - warmup] = now_ns() - start;
            elapsed += samples[i - warmup];
        }
    }
    for (int i = 0; i < batches + warmup; i++) {
        memset(data, i % 251, size);
        if (pread(fd, actual, size, (off_t)i * size) != (ssize_t)size || memcmp(actual, data, size))
            fail("verify batch", EIO);
    }
    qsort(samples, batches, sizeof(*samples), compare);
    printf("%s,%u,%.2f,%.2f,%.2f\n", argv[1], size, batches * 1e9 / elapsed,
           samples[batches / 2] / 1000., samples[batches * 99 / 100 - 1] / 1000.);
    if (use_ring)
        io_uring_queue_exit(&ring);
    return close(fd) < 0;
}
