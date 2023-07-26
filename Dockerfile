FROM python:3.11-slim

ARG RUN_AS_USER
ARG REDIS_URL

RUN set -ex \
 && apt-get update \
 && apt-get upgrade -y \
 && apt-get install -y git gcc htop locales \
 && useradd -ms /bin/bash $RUN_AS_USER \
 && echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen && locale-gen \
# Clean up
 && apt-get autoremove -y \
 && apt-get clean -y \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

COPY redis_stomp /home/$RUN_AS_USER/src/redis_stomp

USER ${RUN_AS_USER}

WORKDIR /home/$RUN_AS_USER/src/redis_stomp

ENV PYTHONPATH="$PYTHONPATH:/home/$RUN_AS_USER/src/"

ENTRYPOINT ["python", "-Wdefault", "/home/$RUN_AS_USER/src/redis_stomp/main.py", "--redis", "$REDIS_URL" ]
