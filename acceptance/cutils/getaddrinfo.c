#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#define KRONOS "kronos"
#define KRONOS_LEN 6
#define MAX_IP_ADDR_LEN 120

// real_getaddrinfo is a pointer to the real getaddrinfo function in the C library
int (*real_getaddrinfo)(const char *node, const char *service,
                        const struct addrinfo *hints,
                        struct addrinfo **res) = NULL;
// real_freeaddrinfo is a pointer to the real freeaddrinfo function in the C library
void (*real_freeaddrinfo)(struct addrinfo *res) = NULL;

void deepCopy(struct addrinfo **from, struct addrinfo **to)
{
    while (from != NULL && *from != NULL)
    {
        struct addrinfo *ai = (struct addrinfo *)malloc(sizeof(struct addrinfo));
        if (ai == NULL)
        {
            return;
        }

        ai->ai_flags = (*from)->ai_flags;
        ai->ai_family = (*from)->ai_family;
        ai->ai_socktype = (*from)->ai_socktype;
        ai->ai_protocol = (*from)->ai_protocol;
        ai->ai_addrlen = (*from)->ai_addrlen;

        if ((*from)->ai_canonname != NULL)
        {
            ai->ai_canonname = (char *)malloc(strlen((*from)->ai_canonname) + 1);
            if (ai->ai_canonname == NULL)
            {
                free(ai);
                return;
            }
            strcpy(ai->ai_canonname, (*from)->ai_canonname);
        }
        else
        {
            ai->ai_canonname = NULL;
        }

        ai->ai_addr = malloc((*from)->ai_addrlen);
        memcpy(ai->ai_addr, (*from)->ai_addr, (*from)->ai_addrlen);
        ai->ai_next = NULL;

        *to = ai;
        to = &ai->ai_next;
        from = &(*from)->ai_next;
    }
}

int getaddrinfo(const char *node, const char *service,
                const struct addrinfo *hints,
                struct addrinfo **res)
{
    if (real_getaddrinfo == NULL)
    {
        real_getaddrinfo = dlsym(RTLD_NEXT, "getaddrinfo");
    }
    if (real_freeaddrinfo == NULL)
    {
        real_freeaddrinfo = dlsym(RTLD_NEXT, "freeaddrinfo");
    }

    // The below conditional code is specific for testing kronos
    // It assumes the following
    // 1. The kronos node id of the src is set in the environment variable KRONOS_NODE_ID
    // 2. The destination (node) is of the form "kronos<id>"
    // 3. The proxy for the destination is at "127.0.<src_id>.<dest_id>"
    // 4. If src is same as dest, then the proxy is at "127.0.0.<id>"
    // 5. If the src is "test" (in cases where test is trying to access the kronos nodes for time etc.),
    //    then the proxy is at "127.0.0.<dest_id>"
    char *kronos_id = getenv("KRONOS_NODE_ID");
    if (kronos_id != NULL && strncmp(node, KRONOS, KRONOS_LEN) == 0)
    {
        struct addrinfo *ai = (struct addrinfo *)malloc(sizeof(struct addrinfo));
        if (ai == NULL)
        {
            return EAI_MEMORY;
        }

        ai->ai_flags = 0;
        ai->ai_family = AF_INET;
        ai->ai_socktype = SOCK_STREAM;
        ai->ai_protocol = IPPROTO_TCP;
        ai->ai_addrlen = sizeof(struct sockaddr_in);
        ai->ai_canonname = NULL;

        struct sockaddr_in *sa = (struct sockaddr_in *)malloc(sizeof(struct sockaddr_in));
        if (sa == NULL)
        {
            free(ai);
            return EAI_MEMORY;
        }

        char *ip_addr = (char *)malloc(MAX_IP_ADDR_LEN);
        if (strncmp(node + KRONOS_LEN, kronos_id, strlen(kronos_id)) == 0)
        {
            // if dest is same as src
            snprintf(ip_addr, MAX_IP_ADDR_LEN, "127.0.0.%s", kronos_id);
        }
        else if (strncmp(kronos_id, "test", strlen("test")) == 0)
        {
            // give actual ip to test, not proxy
            snprintf(ip_addr, MAX_IP_ADDR_LEN, "127.0.0.%s", node + KRONOS_LEN);
        }
        else
        {
            snprintf(ip_addr, MAX_IP_ADDR_LEN, "127.0.%s.%s", kronos_id, node + KRONOS_LEN);
        }
        // Set the sin_family, sin_port, and sin_addr fields
        sa->sin_family = AF_INET;
        sa->sin_addr.s_addr = inet_addr(ip_addr);

        // Set the ai_addr field
        ai->ai_addr = (struct sockaddr *)sa;
        ai->ai_next = NULL;

        // Set the res field
        *res = ai;

        return 0;
    }
    else
    {
        struct addrinfo **stdres = (struct addrinfo **)malloc(sizeof(struct addrinfo *));
        int ret = real_getaddrinfo(node, service, hints, stdres);
        // deep copy the result from stdres to res
        // to free up libc's allocation.
        // This is really inefficient, not recommended for production use
        deepCopy(stdres, res);
        real_freeaddrinfo(*stdres);
        return ret;
    }

    return 0;
}

void freeaddrinfo(struct addrinfo *res)
{
    while (res != NULL)
    {
        struct addrinfo *next = res->ai_next;
        if (res->ai_addr != NULL)
        {
            free(res->ai_addr);
        }
        if (res->ai_canonname != NULL)
        {
            free(res->ai_canonname);
        }
        free(res);
        res = next;
    }
}
