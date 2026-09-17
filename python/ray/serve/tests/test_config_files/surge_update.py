"""A deployment whose replacement replicas wait for a startup signal."""

import os

import ray
from ray import serve
from ray.serve._private.constants import SERVE_NAMESPACE


@serve.deployment
class SurgeUpdate:
    def __init__(self, version: str, startup_gate: str):
        self._version = os.environ.get("SURGE_VERSION", version)
        gate_name = os.environ.get("SURGE_GATE", startup_gate)
        if gate_name:
            gate = ray.get_actor(gate_name, namespace=SERVE_NAMESPACE)
            ray.get(gate.wait.remote())

    def __call__(self, request):
        return self._version


def build(args):
    return SurgeUpdate.bind(args.get("version", "v1"), args.get("startup_gate", ""))
