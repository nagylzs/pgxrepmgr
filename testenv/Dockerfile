FROM postgres:17

# iptables and iproute2 (tc) are used by cluster.sh to cut nodes off and to add network delay.
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-17-repmgr iptables iproute2 \
 && rm -rf /var/lib/apt/lists/*

COPY node-entrypoint.sh /usr/local/bin/node-entrypoint.sh

# Not under the image's VOLUME, so the data goes away with the container.
ENV PGDATA=/var/lib/postgresql/pgdata
USER postgres
ENTRYPOINT ["/usr/local/bin/node-entrypoint.sh"]
