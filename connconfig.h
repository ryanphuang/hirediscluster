#ifndef __CONN_CONFIG_H_
#define __CONN_CONFIG_H_

#include <string>
#include <map>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cctype>
#include <algorithm>
#include <utility>
#include <cstdio>

namespace rediscluster {

  typedef std::vector<std::string> HostList;

  class ConnectionConfig {
    public:
      ConnectionConfig(const char *host, int port) : 
          m_host(host), m_port(port) {}

      inline ConnectionConfig& setHost(const char *host) 
      {
        if (host != NULL) {
          m_host.assign(host);
        }
        return *this;
      }

      inline ConnectionConfig& setPort(int port)
      {
        m_port = port;
        return *this;
      }

      ConnectionConfig& setCluster(const char *cluster)
      {
        m_cluster.clear();
        if (cluster != NULL) {
          std::istringstream iss(cluster);
          std::string host;
          while (getline(iss, host, ',')) {
            std::string shost = trim(host);
            if (!shost.empty()) {
              m_cluster.push_back(host);
            }
          }
        }
        return *this;
      }

      inline ConnectionConfig& setTimeout(double timeout)
      {
        m_timeout = timeout;
        return *this;
      }

      const char *getHost() { return m_host.c_str(); }
      int getPort() { return m_port; }
      double getTimeout() { return m_timeout; }
      HostList & getCluster() { return m_cluster; }

    protected:
      static inline bool NotSpace(char c)
      {
        return !isspace(c);
      }

      static std::string& trim(std::string &s)
      {
        std::string::iterator it = std::find_if(s.begin(), s.end(), NotSpace);
        s.erase(s.begin(), it);
        it = std::find_if(s.rbegin(), s.rend(), NotSpace).base();
        s.erase(it, s.end());
        return s;
      }

    private:
      std::string m_host;
      int m_port;
      double m_timeout;
      HostList m_cluster;
  };

}


#endif /* __CONFIG_H_ */

/* vim: set ts=4 sw=4 : */
