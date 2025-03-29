# SPDX-FileCopyrightText: The RamenDR authors
# SPDX-License-Identifier: Apache-2.0

FROM registry.access.redhat.com/ubi8/ubi-minimal
WORKDIR /
COPY bin/ramen_linux_arm64 /manager

# copy licenses to license folder
RUN mkdir -p licenses
COPY LICENSES/Apache-2.0.txt licenses/Apache-2.0.txt

USER 65532:65532

ENTRYPOINT ["/manager"]
