#ifndef SPS_SESSION_OBSERVER_H
#define SPS_SESSION_OBSERVER_H
#include <stdint.h>
/* Optional inherited AF_UNIX datagram endpoint. Never a public socket or log. */
int spobs_init_fd(int fd);
int spobs_init_env(void);
void spobs_initial(const void *owner, const char *fresh_gid);
void spobs_begin(const void *owner, const char *gid, int supported);
void spobs_end(const void *owner);
void spobs_forget(const void *owner);
void spobs_setup_result(const void *owner, const char *gid, int status, int failed);
void spobs_written(unsigned kind, uint32_t epoch, uint32_t sequence,
                   uint64_t pts, uint32_t frames, uint32_t rtp);
void spobs_volume_text(const void *owner, const char *text);
void spobs_fault(void);
#endif
