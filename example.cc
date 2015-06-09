#include <stdio.h>
#include <stdlib.h>
#include <string>

#include "hirediscluster.h"
#include "connconfig.h"

using namespace rediscluster;
using namespace std;

// a comma separated list of hosts in the cluster
const char * CLUSTER = "localhost";
const int PORT = 7777;
const double TIMEOUT = 1.5;

void write(RedisCluster *client, const char *key, uint32_t keylen, 
            const char *value, uint32_t valuelen)
{
  redisContext *c = client->getClientForKey(key, keylen);
  if (c != NULL) {
    redisReply *reply = client->executeCommand(c,"SET %s %s", 
      string(key, keylen).c_str(), value);
    freeReplyObject(reply);
  }
}

int read(RedisCluster *client, const char *key, uint32_t keylen, 
          char *buff, uint32_t bufflen)
{
  int rd = -1;
  redisContext *c = client->getClientForKey(key, keylen);
  if (c != NULL) {
    redisReply *reply = client->executeCommand(c, 
        "GET %s", string(key, keylen).c_str());
    if (reply->type == REDIS_REPLY_STRING) {
      strncpy(buff, reply->str, bufflen);
      rd = (int) strnlen(buff, bufflen);
    }
    freeReplyObject(reply);
  }
  return rd;
}

int main()
{
  RedisCluster client;
  ConnectionConfig config("", PORT); // use an empty host first;
  config.setCluster(CLUSTER).setTimeout(TIMEOUT); // set cluster and timeout
  if (!client.init(&config)) {
    fprintf(stderr, "fail to initialize redis cluster\n");
    exit(1);
  }
  HostList hosts = config.getCluster();
  if (hosts.empty()) {
    fprintf(stderr, "empty hosts in redis cluster\n");
    exit(1);
  }
  HostList::iterator hit;
  int i = 1;
  for (hit = hosts.begin(); hit != hosts.end(); ++hit) {
    printf("HOST %d: %s\n", i, hit->c_str());
    ++i;
  }

  client.getClientForKey("hello", 5);
  client.getClientForKey("world", 5);
  client.getClientForKey("foo", 3);
  client.getClientForKey("bar", 3);
  client.getClientForKey("barrrr", 6);
  printf("redis driver successfully initialized\n");

  printf("========================================================\n");
  printf("               smoke test redis cluster                 \n");
  printf("========================================================\n");
  char buf[32];
  write(&client, "world", 5, "hello", 6); // also store the '\0' in value
  read(&client, "world", 5, buf, sizeof(buf));
  printf("[world]=%s\n", buf);
  printf("========================================================\n");
}
