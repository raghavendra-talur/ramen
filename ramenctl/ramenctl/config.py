# SPDX-FileCopyrightText: The RamenDR authors
# SPDX-License-Identifier: Apache-2.0

import drenv
from drenv import kubectl

from . import minio
from . import command


def register(commands):
    parser = commands.add_parser(
        "config",
        help="Configure ramen hub operator",
    )
    parser.set_defaults(func=run)
    command.add_common_arguments(parser)
    command.add_ramen_arguments(parser)


def run(args):
    env = command.env_info(args)

    command.info("Waiting until ramen-hub-operator is rolled out")
    kubectl.rollout(
        "status",
        "deploy/ramen-hub-operator",
        f"--namespace={args.ramen_namespace}",
        "--timeout=180s",
        context=env["hub"],
        log=command.debug,
    )

    command.info("Creating s3 secret in ramen hub namespace")
    kubectl.apply(
        "--filename",
        command.resource("s3-secret.yaml"),
        context=env["hub"],
        log=command.debug,
    )

    command.info("Updating ramen config map")
    template = drenv.template(command.resource("configmap.yaml"))
    yaml = template.substitute(
        auto_deploy="true",
        minio_url_dr1=minio.service_url(env["clusters"][0]),
        minio_url_dr2=minio.service_url(env["clusters"][1]),
    )
    kubectl.apply(
        "--filename=-",
        input=yaml,
        context=env["hub"],
        log=command.debug,
    )

    command.info("Creating DRClusters and DRPolicy for %s", env["topology"])
    kubectl.apply(
        "--kustomize",
        command.resource(env["topology"]),
        context=env["hub"],
        log=command.debug,
    )

    command.info("Waiting until DRClusters report phase")
    for name in env["clusters"]:
        drenv.wait_for(
            f"drcluster/{name}",
            output="jsonpath={.status.phase}",
            namespace=args.ramen_namespace,
            timeout=180,
            profile=env["hub"],
            log=command.debug,
        )

    command.info("Waiting until DRClusters phase is available")
    kubectl.wait(
        "drcluster",
        "--all",
        "--for=jsonpath={.status.phase}=Available",
        f"--namespace={args.ramen_namespace}",
        context=env["hub"],
        log=command.debug,
    )

    command.info("Waiting until DRPolicy is validated")
    kubectl.wait(
        "drpolicy/dr-policy",
        "--for=condition=Validated",
        f"--namespace={args.ramen_namespace}",
        context=env["hub"],
        log=command.debug,
    )