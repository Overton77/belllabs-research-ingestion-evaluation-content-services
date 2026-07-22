FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Cloud Agents run this image inside a VM/container boundary. Docker uses
# fuse-overlayfs and legacy iptables so nested Compose and sandbox containers
# work reliably in that environment.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        fuse-overlayfs \
        git \
        gnupg \
        iptables \
        openssh-client \
        python3.12 \
        python3.12-dev \
        python3.12-venv \
        sudo \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl --retry 3 --retry-delay 5 -fsSL \
        https://download.docker.com/linux/ubuntu/gpg \
        | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu noble stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        containerd.io \
        docker-buildx-plugin \
        docker-ce \
        docker-ce-cli \
        docker-compose-plugin \
    && update-alternatives --set iptables /usr/sbin/iptables-legacy \
    && update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy \
    && mkdir -p /etc/docker \
    && printf '%s\n' \
        '{' \
        '  "storage-driver": "fuse-overlayfs"' \
        '}' > /etc/docker/daemon.json \
    && rm -rf /var/lib/apt/lists/*

RUN id -u ubuntu >/dev/null 2>&1 || useradd -m -s /bin/bash ubuntu \
    && groupadd -f docker \
    && usermod -aG docker,sudo ubuntu \
    && echo "ubuntu ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/ubuntu \
    && chmod 0440 /etc/sudoers.d/ubuntu \
    && mkdir -p /biotech-meta \
    && chown ubuntu:ubuntu /biotech-meta

RUN curl --retry 3 --retry-delay 5 -LsSf https://astral.sh/uv/install.sh \
        | env UV_INSTALL_DIR=/usr/local/bin sh \
    && uv --version \
    && docker --version \
    && docker compose version

USER ubuntu
WORKDIR /workspace
