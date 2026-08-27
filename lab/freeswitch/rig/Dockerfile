# docker-compose.microsip.yml wants `rig-freeswitch:latest`, an image that has
# never existed in this repo. This builds one from whatever FreeSWITCH image a
# deployment already has, and reconciles the three ways a source-built
# FreeSWITCH differs from the Debian-packaged one the lab config assumes.
#
#   docker build -t rig-freeswitch:latest \
#     --build-arg BASE=my-freeswitch:tag lab/freeswitch/rig
#
# Everything here was found by running the microsip lab against a real
# deployment image on 2026-08-27; docs/lab.md records what each line cost.
ARG BASE=freeswitch:v1.10.12_v1.2.7_v2.0.10_AI
FROM ${BASE}

# 1. A source build keeps its binaries in /usr/local/freeswitch/bin and puts
#    nothing on PATH. lab/fs_parity_drill.sh calls bare `fs_cli`, so without
#    these links it found no channel for the Call-ID and reported that
#    FreeSWITCH was not in the path of a call FreeSWITCH had just answered.
RUN ln -sf /usr/local/freeswitch/bin/fs_cli /usr/local/bin/fs_cli \
 && ln -sf /usr/local/freeswitch/bin/freeswitch /usr/local/bin/freeswitch

# 2. FreeSWITCH does not merge two contexts of the same name, and it starts
#    every profile in sip_profiles/. A deployment image ships its own, which
#    contend with the lab's for port 5060 and route calls out of a context the
#    lab dialplan is not in. The lab mounts its own single profile and single
#    dialplan over these paths, so the image's copies are dropped here.
RUN rm -f /usr/local/freeswitch/conf/sip_profiles/*.xml \
          /usr/local/freeswitch/conf/dialplan/*.xml

# 3. lab/freeswitch/internal.xml reads $${external_ip}, which no vanilla
#    vars.xml defines -- FreeSWITCH left it unexpanded and the profile
#    advertised a literal. Inside the lab network the container's own address
#    is the right answer.
RUN sed -i 's#</include>#  <X-PRE-PROCESS cmd="set" data="external_ip=$${local_ip_v4}"/>\n</include>#' \
      /usr/local/freeswitch/conf/vars.xml

# 4. The bundled event_socket.conf.xml binds listen-ip "::". This container has
#    no IPv6 at all (/proc/net/tcp6 is empty), so mod_event_socket bound
#    nothing and fs_cli could not connect -- which is every drill that drives
#    FreeSWITCH over ESL.
RUN sed -i 's#<param name="listen-ip" value="::"/>#<param name="listen-ip" value="0.0.0.0"/>#' \
      /usr/local/freeswitch/conf/autoload_configs/event_socket.conf.xml
