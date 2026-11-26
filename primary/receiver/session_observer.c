/* Bounded optional local evidence; no RTSP payload/header/identity logging.
 * This channel attests process lineage, not an authenticated remote sender.
 * Write failure disables evidence permanently; audio behavior is unchanged. */
#define _POSIX_C_SOURCE 200809L
#include "session_observer.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <ctype.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

enum { LIVE=1, GID=2, SETUP=4, SUPPORTED=8, BROKEN=16 };
enum { HELLO=1, BEGIN=2, ACCEPT=3, FRAME=4, END=5, FAULT=6, VOLUME=7 };
static pthread_mutex_t lock=PTHREAD_MUTEX_INITIALIZER;
static int channel=-1, initialized=0;
static const void *owner;
static const void *pending_owner;
static const void *pending_volume_owner;
static unsigned char pending_group[16];
static int pending_valid,pending_accepted;
static unsigned flags;
static uint64_t serial,generation,last_emit;
static uint32_t epoch,first_sequence,sequence,kind,frames,rtp;
static uint64_t pts;
static unsigned char group[16];
static int32_t volume_milli,pending_volume_milli;
static uint32_t volume_revision,pending_volume_revision;

static void u32(unsigned char *p,uint32_t v) {
  p[0]=v>>24;p[1]=v>>16;p[2]=v>>8;p[3]=v;
}
static void u64(unsigned char *p,uint64_t v) {u32(p,v>>32);u32(p+4,v);}
static uint64_t now_ns(void) {
  struct timespec t;
  if(clock_gettime(CLOCK_MONOTONIC,&t))return 0;
  return (uint64_t)t.tv_sec*1000000000+(uint64_t)t.tv_nsec;
}
static int nibble(unsigned char c) {
  if(c>='0' && c<='9')return c-'0';
  if(c>='a' && c<='f')return c-'a'+10;
  if(c>='A' && c<='F')return c-'A'+10;
  return -1;
}
static int uuid(const char *s,unsigned char out[16]) {
  if(!s || strnlen(s,37)!=36)return 0;
  unsigned n=0,nonzero=0;
  for(unsigned i=0;i<36;) {
    if(i==8 || i==13 || i==18 || i==23) {if(s[i++]!='-')return 0;continue;}
    int hi=nibble(s[i++]),lo=nibble(s[i++]);
    if(hi<0 || lo<0 || n>=16)return 0;
    out[n++]=(unsigned char)((hi<<4)|lo);nonzero|=out[n-1];
  }
  return n==16 && nonzero;
}
static void close_locked(void) {
  if(channel>=0)close(channel);
  channel=-1;flags=BROKEN;memset(group,0,sizeof(group));
}
static void emit_locked(unsigned event) {
  if(channel<0)return;
  uint64_t at=now_ns();
  if(!at || serial==UINT64_MAX){close_locked();return;}
  unsigned char b[88]={0};memcpy(b,"SPSE",4);b[4]=2;b[5]=event;b[7]=flags;
  u64(b+8,++serial);u64(b+16,generation);u64(b+24,at);
  u32(b+32,epoch);u32(b+36,first_sequence);u32(b+40,sequence);u32(b+44,kind);
  u64(b+48,pts);u32(b+56,frames);u32(b+60,rtp);memcpy(b+64,group,16);
  u32(b+80,(uint32_t)volume_milli);u32(b+84,volume_revision);
  int send_flags=0; /* descriptor is O_NONBLOCK on every supported OS */
#ifdef MSG_DONTWAIT
  send_flags|=MSG_DONTWAIT;
#endif
#ifdef MSG_NOSIGNAL
  send_flags|=MSG_NOSIGNAL;
#endif
  ssize_t sent=send(channel,b,sizeof(b),send_flags);
  /* Datagram send is atomic. No retry, partial record, queue or log. */
  if(sent!=(ssize_t)sizeof(b))close_locked();else last_emit=at;
}
static void fault_locked(void) {flags=BROKEN;memset(group,0,16);emit_locked(FAULT);close_locked();}
static void enter(int *old) {pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,old);pthread_mutex_lock(&lock);}
static void leave(int old) {pthread_mutex_unlock(&lock);pthread_setcancelstate(old,NULL);}

int spobs_init_fd(int fd) {
  int old;enter(&old);
  if(initialized){leave(old);return -1;}initialized=1;
  struct sockaddr_un address; socklen_t length=sizeof(address),size=sizeof(int);int type=0;
  if(fd<3 || getsockopt(fd,SOL_SOCKET,SO_TYPE,&type,&size) || type!=SOCK_DGRAM ||
     getsockname(fd,(struct sockaddr *)&address,&length) || address.sun_family!=AF_UNIX ||
     getpeername(fd,(struct sockaddr *)&address,&length) || address.sun_family!=AF_UNIX) {
    leave(old);return -1;
  }
  int existing=fcntl(fd,F_GETFD),mode=fcntl(fd,F_GETFL);
  if(existing<0 || mode<0 || fcntl(fd,F_SETFD,existing|FD_CLOEXEC) ||
     fcntl(fd,F_SETFL,mode|O_NONBLOCK)) {leave(old);return -1;}
#ifdef SO_NOSIGPIPE
  int yes=1;
  if(setsockopt(fd,SOL_SOCKET,SO_NOSIGPIPE,&yes,sizeof(yes))){leave(old);return -1;}
#endif
  channel=fd;emit_locked(HELLO);int ok=channel>=0;leave(old);return ok?0:-1;
}
int spobs_init_env(void) {
  const char *s=getenv("SPS_SELECTION_FD");
  if(!s)return 0;
  if(strnlen(s,11)==0 || strnlen(s,11)>10)return -1;
  for(const char *p=s;*p;p++)if(*p<'0'||*p>'9')return -1;
  errno=0;char *end=NULL;long fd=strtol(s,&end,10);
  if(errno || *end || fd<3 || fd>INT_MAX)return -1;
  int result=spobs_init_fd((int)fd);
  unsetenv("SPS_SELECTION_FD"); /* start/stop helper execs cannot inherit it */
  return result;
}
void spobs_begin(const void *new_owner,const char *gid,int supported) {
  int old;enter(&old);
  if(channel<0){leave(old);return;}
  if(!new_owner || owner || generation==UINT64_MAX){fault_locked();leave(old);return;}
  volume_milli=pending_volume_owner==new_owner?pending_volume_milli:0;
  volume_revision=pending_volume_owner==new_owner?pending_volume_revision:0;
  owner=new_owner;generation++;flags=LIVE|(supported?SUPPORTED:0);
  memset(group,0,16);unsigned char parsed[16];
  if(supported && pending_owner==new_owner && pending_valid && pending_accepted &&
     uuid(gid,parsed) && !memcmp(parsed,pending_group,16)) {
    memcpy(group,parsed,16);flags|=GID;
  }
  epoch=first_sequence=sequence=kind=frames=rtp=0;pts=0;
  emit_locked(BEGIN);leave(old);
}
void spobs_setup_result(const void *which,const char *gid,int status,int failed) {
  int old;enter(&old);
  if(channel>=0 && pending_owner==which && owner!=which) {
    unsigned char parsed[16];
    pending_accepted=!failed && status==200 && pending_valid && uuid(gid,parsed) &&
                     !memcmp(parsed,pending_group,16);
  }
  if(channel>=0 && owner==which) {
    unsigned char parsed[16];
    if(failed || status!=200 || !(flags&GID) || !uuid(gid,parsed) || memcmp(group,parsed,16))
      fault_locked();
    else {flags|=SETUP;emit_locked(ACCEPT);}
  }
  leave(old);
}
void spobs_initial(const void *which,const char *gid) {
  int old;enter(&old);
  if(channel>=0) {
    if(owner==which)fault_locked();
    if(pending_volume_owner!=which){pending_volume_milli=0;pending_volume_revision=0;pending_volume_owner=which;}
    pending_owner=which;pending_accepted=0;
    memset(pending_group,0,16);pending_valid=uuid(gid,pending_group);
  }
  leave(old);
}
void spobs_forget(const void *which) {
  int old;enter(&old);
  if(channel>=0 && owner==which) {flags&=~LIVE;emit_locked(END);owner=NULL;}
  if(pending_volume_owner==which){pending_volume_owner=NULL;pending_volume_revision=0;pending_volume_milli=0;}
  if(pending_owner==which){pending_owner=NULL;pending_valid=pending_accepted=0;memset(pending_group,0,16);}
  leave(old);
}
void spobs_written(unsigned k,uint32_t e,uint32_t s,uint64_t p,uint32_t f,uint32_t r) {
  int old;enter(&old);
  if(channel<0){leave(old);return;}
  if(!owner || !(flags&LIVE) || !s || s<=sequence || k<1 || k>4 ||
     (k==3 ? (!e || e<=epoch || p || f || r) : (e!=epoch || !first_sequence)) ||
     (k==4 && (p||f||r)) || (k<=2 && (!f || f>8192)) ||
     (k==1 && !p) || (k==2 && p)) {fault_locked();leave(old);return;}
  if(k==3){epoch=e;first_sequence=s;}
  sequence=s;kind=k;pts=p;frames=f;rtp=r;
  if(k==4)flags&=~LIVE;
  uint64_t at=now_ns();
  if(k>=3 || !last_emit || at-last_emit>=250000000)emit_locked(FRAME);
  leave(old);
}
void spobs_end(const void *which) {
  int old;enter(&old);
  if(channel>=0 && owner==which) {flags&=~LIVE;emit_locked(END);owner=NULL;}
  leave(old);
}
void spobs_fault(void) {int old;enter(&old);fault_locked();leave(old);}

void spobs_volume_text(const void *which,const char *text) {
  if(!text || strnlen(text,65)>64)return;
  errno=0;char *end;double value=strtod(text,&end);
  if(errno || end==text || !isfinite(value))return;
  while(*end && isspace((unsigned char)*end))end++;
  if(*end || !(value==-144.0 || (value>=-30.0 && value<=0.0)))return;
  int32_t milli=(int32_t)llround(value*1000.0);
  int old;enter(&old);
  if(channel>=0 && owner==which && (flags&LIVE)) {
    if(!volume_revision || milli!=volume_milli) {
      if(volume_revision==UINT32_MAX){fault_locked();leave(old);return;}
      volume_milli=milli;volume_revision++;
      pending_volume_owner=which;pending_volume_milli=milli;pending_volume_revision=volume_revision;
      emit_locked(VOLUME);
    }
  } else if(channel>=0) {
    if(pending_volume_owner!=which){pending_volume_owner=which;pending_volume_revision=0;pending_volume_milli=0;}
    if(!pending_volume_revision || pending_volume_milli!=milli) {
      if(pending_volume_revision==UINT32_MAX){fault_locked();leave(old);return;}
      pending_volume_milli=milli;pending_volume_revision++;
    }
  }
  leave(old);
}
