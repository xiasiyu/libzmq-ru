/* SPDX-License-Identifier: MPL-2.0 */
#ifndef RU_LIBZMQ_ZMQ_H_INCLUDED
#define RU_LIBZMQ_ZMQ_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

#if defined _WIN32
#if defined ZMQ_STATIC
#define ZMQ_EXPORT
#else
#define ZMQ_EXPORT __declspec(dllexport)
#endif
#else
#define ZMQ_EXPORT __attribute__((visibility("default")))
#endif

#define ZMQ_VERSION_MAJOR 4
#define ZMQ_VERSION_MINOR 3
#define ZMQ_VERSION_PATCH 6
#define ZMQ_MAKE_VERSION(major, minor, patch) ((major) * 10000 + (minor) * 100 + (patch))
#define ZMQ_VERSION ZMQ_MAKE_VERSION(ZMQ_VERSION_MAJOR, ZMQ_VERSION_MINOR, ZMQ_VERSION_PATCH)

#define ZMQ_HAUSNUMERO 156384712
#ifndef ENOTSUP
#define ENOTSUP (ZMQ_HAUSNUMERO + 1)
#endif
#ifndef ENOTSOCK
#define ENOTSOCK (ZMQ_HAUSNUMERO + 9)
#endif
#ifndef EFSM
#define EFSM (ZMQ_HAUSNUMERO + 51)
#endif
#ifndef ENOCOMPATPROTO
#define ENOCOMPATPROTO (ZMQ_HAUSNUMERO + 52)
#endif
#ifndef ETERM
#define ETERM (ZMQ_HAUSNUMERO + 53)
#endif
#ifndef EMTHREAD
#define EMTHREAD (ZMQ_HAUSNUMERO + 54)
#endif

#define ZMQ_PAIR 0
#define ZMQ_PUB 1
#define ZMQ_SUB 2
#define ZMQ_REQ 3
#define ZMQ_REP 4
#define ZMQ_DEALER 5
#define ZMQ_ROUTER 6
#define ZMQ_PULL 7
#define ZMQ_PUSH 8
#define ZMQ_XPUB 9
#define ZMQ_XSUB 10
#define ZMQ_STREAM 11
#define ZMQ_SERVER 12
#define ZMQ_CLIENT 13
#define ZMQ_RADIO 14
#define ZMQ_DISH 15
#define ZMQ_GATHER 16
#define ZMQ_SCATTER 17
#define ZMQ_DGRAM 18
#define ZMQ_PEER 19
#define ZMQ_CHANNEL 20

#define ZMQ_DONTWAIT 1
#define ZMQ_SNDMORE 2
#define ZMQ_MORE 1

typedef struct zmq_msg_t {
#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_ARM64))
    __declspec(align(8)) unsigned char _[64];
#elif defined(_MSC_VER)
    __declspec(align(4)) unsigned char _[64];
#elif defined(__GNUC__) || defined(__clang__)
    unsigned char _[64] __attribute__((aligned(sizeof(void *))));
#else
    unsigned char _[64];
#endif
} zmq_msg_t;

typedef void(zmq_free_fn)(void *data_, void *hint_);

ZMQ_EXPORT int zmq_errno(void);
ZMQ_EXPORT const char *zmq_strerror(int errnum_);
ZMQ_EXPORT void zmq_version(int *major_, int *minor_, int *patch_);

ZMQ_EXPORT void *zmq_ctx_new(void);
ZMQ_EXPORT int zmq_ctx_term(void *context_);
ZMQ_EXPORT int zmq_ctx_shutdown(void *context_);
ZMQ_EXPORT int zmq_ctx_set(void *context_, int option_, int optval_);
ZMQ_EXPORT int zmq_ctx_get(void *context_, int option_);

ZMQ_EXPORT void *zmq_init(int io_threads_);
ZMQ_EXPORT int zmq_term(void *context_);
ZMQ_EXPORT int zmq_ctx_destroy(void *context_);

ZMQ_EXPORT void *zmq_socket(void *context_, int type_);
ZMQ_EXPORT int zmq_close(void *socket_);
ZMQ_EXPORT int zmq_bind(void *socket_, const char *addr_);
ZMQ_EXPORT int zmq_connect(void *socket_, const char *addr_);
ZMQ_EXPORT int zmq_send(void *socket_, const void *buf_, size_t len_, int flags_);
ZMQ_EXPORT int zmq_recv(void *socket_, void *buf_, size_t len_, int flags_);

ZMQ_EXPORT int zmq_msg_init(zmq_msg_t *msg_);
ZMQ_EXPORT int zmq_msg_init_size(zmq_msg_t *msg_, size_t size_);
ZMQ_EXPORT int zmq_msg_init_data(zmq_msg_t *msg_, void *data_, size_t size_, zmq_free_fn *ffn_, void *hint_);
ZMQ_EXPORT int zmq_msg_close(zmq_msg_t *msg_);
ZMQ_EXPORT void *zmq_msg_data(zmq_msg_t *msg_);
ZMQ_EXPORT size_t zmq_msg_size(const zmq_msg_t *msg_);

#undef ZMQ_EXPORT

#ifdef __cplusplus
}
#endif

#endif
