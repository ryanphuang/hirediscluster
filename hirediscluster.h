#ifndef __HIREDISCLUSTER_H_
#define __HIREDISCLUSTER_H_

#include <hiredis.h>
#include <map>
#include <utility>
#include <vector>
#include <string.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <math.h>
#include <algorithm>
#include <sstream>

#include "connconfig.h"
#include "rediscutil.h"

#define CLUSTER_SLOTS_CMD    "CLUSTER SLOTS"
// #define DEBUG_SLOT

namespace rediscluster {

  struct KeySlot;
  struct KeySlotComp;
  struct KeyHostComp;

  typedef std::pair<const char *, int> KeyHost;
  typedef std::vector<redisContext *> RClients;
  typedef std::map<KeyHost, redisContext *, KeyHostComp> RClientConnMap;
  typedef std::map<KeySlot, redisContext *, KeySlotComp> RClientSlotMap;

  typedef struct KeySlot {
    uint32_t start;
    uint32_t end;
  } KeySlot;

  typedef struct KeySlotComp {
    bool operator() (const KeySlot &lhs, const KeySlot &rhs) const 
    { 
      return lhs.start < rhs.start; 
    }
  } KeySlotComp;

  typedef struct KeyHostComp {
    bool operator() (const KeyHost &lhs, const KeyHost &rhs) const
    {
      int cmp = strcmp(lhs.first, rhs.first); 
      if (cmp == 0)
        return lhs.second < rhs.second;
      return cmp < 0;
    }

  } KeyHostComp;

  class RedisCluster {
    public:
      RedisCluster() 
      { 
        m_initialized = false;
      }

      ~RedisCluster()
      {
        RClientConnMap::iterator rit;
        for (rit = m_clientconn_map.begin(); rit != m_clientconn_map.end(); ++rit) {
          redisContext *ctx = rit->second;
          if (ctx != NULL)
            redisFree(ctx);
        }
        m_clientconn_map.clear();
      }

      bool init(ConnectionConfig *config)
      {
        if (m_initialized) {
          fprintf(stderr, "redis cluster is already set\n");
          return false;
        }

        HostList hosts = config->getCluster();
        if (hosts.empty()) {
          fprintf(stderr, "Empty hosts to create redis cluster\n");
          return false;
        }

        int port = config->getPort();
        double timeout = config->getTimeout();
        int sec = (int) floor(timeout);
        int usec = (int) ((timeout - sec) * 1000000);
        struct timeval tv = {sec, usec};

        HostList::iterator hit;
        for (hit = hosts.begin(); hit != hosts.end(); ++hit) {
          const char *host;
          char ip[32];
          if (RedisClusterUtil::resolve(hit->c_str(), ip, 32)) {
            host = ip;
          } else {
            host = hit->c_str();
          }
          redisContext* ctx = connect(host, port, &tv);
          if (ctx == NULL)
            return false;
        }
        m_initialized = true;
        if (!buildSlots()) {
          fprintf(stderr, "Fail to build slot map for redis cluster\n");
          m_initialized = false;
          return false;
        }
        return true;
      }

      redisContext *connect(const char *host, int port, struct timeval *tv)
      {
        redisContext* ctx;
        if (tv == NULL || (tv->tv_sec == 0 && tv->tv_usec == 0))
          ctx = redisConnect(host, port); // no timeout specified
        else
          ctx = redisConnectWithTimeout(host, port, *tv); 
        if (ctx == NULL || ctx->err) {
          if (ctx) {
            fprintf(stderr, "Error connecting '%s:%d': %s\n", host, port, ctx->errstr);
            redisFree(ctx);
          } else {
            fprintf(stderr, "Fail to create redis context for '%s:%d'\n", host, port);
          }
          return NULL;
        }
        // update the connection map
        m_clientconn_map.insert(std::make_pair(std::make_pair(
            ctx->tcp.host, ctx->tcp.port), ctx));
        // printf("Created redis context for %s:%d\n", ctx->tcp.host, ctx->tcp.port);
        return ctx;
      }

      bool buildSlots()
      {
        if (!m_initialized) {
          fprintf(stderr, "Cannot build slots: cluster uninitialized\n");
          return false;
        }
        if (m_clientconn_map.empty()) {
          fprintf(stderr, "Cannot build slots: empty cluster\n");
          return false;
        }
        RClientConnMap::iterator rit;
        bool found = false;
        for (rit = m_clientconn_map.begin(); rit != m_clientconn_map.end(); ++rit) {
          redisContext *ctx = rit->second;
          if (ctx == NULL)
              continue;
          redisReply *reply;
          reply = (redisReply *) redisCommand(ctx, CLUSTER_SLOTS_CMD);
          if (reply->type == REDIS_REPLY_ARRAY) {
            unsigned int i;
            for (i = 0; i < reply->elements; ++i) {
              redisReply *rele = reply->element[i];
              if (rele->type == REDIS_REPLY_ARRAY && 
                  rele->elements >= 3 && 
                  rele->element[0]->type == REDIS_REPLY_INTEGER &&
                  rele->element[1]->type == REDIS_REPLY_INTEGER &&
                  rele->element[2]->type == REDIS_REPLY_ARRAY &&
                  rele->element[2]->element[0]->type == REDIS_REPLY_STRING &&
                  rele->element[2]->element[1]->type == REDIS_REPLY_INTEGER) {
                KeySlot slot;
                RClientConnMap::iterator cit;

                slot.start = (uint32_t) (rele->element[0]->integer);
                slot.end = (uint32_t) (rele->element[1]->integer);
                const char * host = rele->element[2]->element[0]->str;
                int port = (int) (rele->element[2]->element[1]->integer);

#ifdef DEBUG_SLOT
                printf("slot [%u-%u] is served by %s:%d\n", 
                    slot.start, slot.end, host, port);
#endif

                redisContext *sc = NULL;
                cit = m_clientconn_map.find(std::make_pair(host, port));

                // if no context object, we need to connect
                if (cit == m_clientconn_map.end())
                  sc = connect(host, port, NULL);  
                else
                  sc = cit->second;
                if (sc != NULL) {
                  m_clientslot_map.insert(std::make_pair(slot, sc));
                  found = true;
                }
              } 
            }
          }
          freeReplyObject(reply);
          if (found)
            break;
        }

#ifdef DEBUG_SLOT
        RClientSlotMap::iterator rsit;
        int i = 0;
        for (rsit = m_clientslot_map.begin(); rsit != m_clientslot_map.end(); ++rsit) {
          printf("slot_map.%d = [%u-%u]\n", i, rsit->first.start, rsit->first.end);
          ++i;
        }
#endif
        return found;
      }

      void getClients(std::vector<redisContext *> &clients)
      {
        RClientConnMap::iterator rit;
        for (rit = m_clientconn_map.begin(); rit != m_clientconn_map.end(); ++rit) {
          redisContext *ctx = rit->second;
          if (ctx != NULL)
            clients.push_back(ctx);
        }
      }

      redisContext *getClientForKey(const char *key, uint32_t keylen)
      {
        if (m_clientslot_map.empty()) {
          if (m_clientconn_map.empty()) {
            printf("Error: no client to return, empty slot map and connection map\n");
            return NULL;
          }
          printf("Warning: empty slot map, returning first (if any) client\n");
          return m_clientconn_map.begin()->second;
        }

        unsigned int slot = RedisClusterUtil::HASH_SLOT(key, keylen);
#ifdef DEBUG_SLOT
        printf("slot('%s') = %u\n", key, slot);
#endif
        RClientSlotMap::iterator it;
        KeySlot ks = {slot, slot};
        it = m_clientslot_map.lower_bound(ks);
        if (it == m_clientslot_map.end() || it->first.start > slot)
          --it;
        if (it->first.start <= slot && slot <= it->first.end) {
          redisContext *ctx = it->second;
#ifdef DEBUG_SLOT
          printf("slot %u is served by %s:%d [%u-%u]\n", slot, ctx->tcp.host, 
              ctx->tcp.port, it->first.start, it->first.end);
#endif
          return ctx;
        } else {
#ifdef DEBUG_SLOT
          printf("slot %u is not in near range [%u-%u]\n", slot, it->first.start, 
              it->first.end);
#endif
          return NULL;
        }
      }

      redisReply *executeCommand(redisContext *context, const char *format, ...)
      {
        va_list arg;
        va_start(arg, format);
        redisReply *reply = (redisReply *) redisvCommand(context, format, arg);
        va_end(arg);
        if (reply->type == REDIS_REPLY_ERROR) {
          std::string error(reply->str, reply->len);
          if (error.compare(0, 5, "MOVED") == 0) {
            size_t host_pos = error.find_last_of(' ') + 1;
            size_t port_pos = error.find_last_of(':') + 1;
            std::string shost = error.substr(host_pos, port_pos - host_pos - 1);
            std::string sport = error.substr(port_pos);
            std::stringstream ss(sport);
            int port;
            ss >> port;
            printf("command should be redirected to %s:%d\n", shost.c_str(), port);
            redisContext *newc = connect(shost.c_str(), port, NULL);
            if (newc != NULL) {
              va_start(arg, format);
              redisReply *newreply = (redisReply *) redisvCommand(newc,format, arg);
              va_end(arg);
              freeReplyObject(reply); // free old reply object
              reply = newreply;
            }
          }
        }
        return reply;
      }

    protected:
      bool m_initialized;
      RClientConnMap m_clientconn_map;
      RClientSlotMap m_clientslot_map;
  };
}

#endif /* __HIREDISCLUSTER_H_ */

/* vim: set ts=4 sw=4 : */
