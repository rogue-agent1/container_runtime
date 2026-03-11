#!/usr/bin/env python3
"""container_runtime.py — Container runtime simulator.

Simulates Linux container primitives: namespaces (PID, NET, MNT, UTS),
cgroups (CPU, memory limits), overlay filesystem, image layers,
and container lifecycle (create, start, stop, exec).

One file. Zero deps. Does one thing well.
"""

import os
import sys
import json
import hashlib
import tempfile
from dataclasses import dataclass, field
from enum import Enum, auto


class ContainerState(Enum):
    CREATED = auto(); RUNNING = auto(); STOPPED = auto(); PAUSED = auto()


@dataclass
class Namespace:
    kind: str  # pid, net, mnt, uts, ipc, user
    id: int = 0

    def __repr__(self): return f"ns:{self.kind}[{self.id}]"


@dataclass
class CgroupLimits:
    cpu_shares: int = 1024       # relative weight
    cpu_quota_us: int = -1       # -1 = unlimited
    cpu_period_us: int = 100000
    memory_limit_bytes: int = -1 # -1 = unlimited
    memory_swap_bytes: int = -1
    pids_max: int = -1

    @property
    def cpu_percent(self) -> float:
        if self.cpu_quota_us <= 0: return 100.0
        return (self.cpu_quota_us / self.cpu_period_us) * 100


@dataclass
class Layer:
    id: str
    files: dict[str, bytes] = field(default_factory=dict)
    deleted: set[str] = field(default_factory=set)  # whiteout files


@dataclass
class Image:
    name: str
    tag: str = 'latest'
    layers: list[Layer] = field(default_factory=list)
    config: dict = field(default_factory=dict)

    @property
    def id(self) -> str:
        content = json.dumps([l.id for l in self.layers]).encode()
        return hashlib.sha256(content).hexdigest()[:12]

    def flatten(self) -> dict[str, bytes]:
        """Merge all layers (overlay fs simulation)."""
        merged = {}
        for layer in self.layers:
            for path in layer.deleted:
                merged.pop(path, None)
            merged.update(layer.files)
        return merged


@dataclass
class Process:
    pid: int
    command: list[str]
    env: dict[str, str] = field(default_factory=dict)
    cwd: str = '/'
    running: bool = True


@dataclass
class Container:
    id: str
    name: str
    image: Image
    state: ContainerState = ContainerState.CREATED
    namespaces: list[Namespace] = field(default_factory=list)
    cgroup: CgroupLimits = field(default_factory=CgroupLimits)
    filesystem: dict[str, bytes] = field(default_factory=dict)
    rw_layer: Layer = field(default_factory=lambda: Layer('rw'))
    processes: list[Process] = field(default_factory=list)
    hostname: str = ''
    env: dict[str, str] = field(default_factory=dict)
    ports: dict[int, int] = field(default_factory=dict)  # host:container
    next_pid: int = 1
    logs: list[str] = field(default_factory=list)

    def effective_fs(self) -> dict[str, bytes]:
        """Overlay: image layers + rw layer."""
        fs = self.image.flatten()
        for path in self.rw_layer.deleted:
            fs.pop(path, None)
        fs.update(self.rw_layer.files)
        return fs


_next_ns_id = 0
def _new_ns_id() -> int:
    global _next_ns_id
    _next_ns_id += 1
    return _next_ns_id


class Runtime:
    """Container runtime managing images and containers."""

    def __init__(self):
        self.images: dict[str, Image] = {}
        self.containers: dict[str, Container] = {}
        self._next_id = 0

    def _gen_id(self) -> str:
        self._next_id += 1
        return hashlib.sha256(str(self._next_id).encode()).hexdigest()[:12]

    def build_image(self, name: str, layers: list[dict[str, bytes]], config: dict = None) -> Image:
        img_layers = []
        for i, files in enumerate(layers):
            lid = hashlib.sha256(json.dumps(sorted(files.keys())).encode()).hexdigest()[:12]
            img_layers.append(Layer(lid, dict(files)))
        img = Image(name, 'latest', img_layers, config or {})
        self.images[f"{name}:latest"] = img
        return img

    def create(self, image_name: str, name: str = '', **kwargs) -> Container:
        img = self.images.get(image_name) or self.images.get(f"{image_name}:latest")
        if not img:
            raise ValueError(f"Image not found: {image_name}")

        cid = self._gen_id()
        hostname = name or cid[:8]

        # Create namespaces
        namespaces = [Namespace(kind, _new_ns_id()) for kind in ['pid', 'net', 'mnt', 'uts', 'ipc']]

        # Cgroup limits
        cgroup = CgroupLimits(
            cpu_shares=kwargs.get('cpu_shares', 1024),
            memory_limit_bytes=kwargs.get('memory_limit', -1),
            pids_max=kwargs.get('pids_max', -1),
        )

        container = Container(
            id=cid, name=name or cid[:8], image=img,
            namespaces=namespaces, cgroup=cgroup,
            hostname=hostname,
            env={**img.config.get('env', {}), **kwargs.get('env', {})},
            ports=kwargs.get('ports', {}),
        )
        self.containers[cid] = container
        return container

    def start(self, container_id: str) -> Container:
        c = self.containers[container_id]
        if c.state == ContainerState.RUNNING:
            raise RuntimeError("Already running")
        c.state = ContainerState.RUNNING
        # Init process (PID 1)
        cmd = c.image.config.get('cmd', ['/bin/sh'])
        entrypoint = c.image.config.get('entrypoint', [])
        full_cmd = entrypoint + cmd
        c.processes.append(Process(1, full_cmd, dict(c.env)))
        c.next_pid = 2
        c.logs.append(f"Started with PID 1: {' '.join(full_cmd)}")
        return c

    def stop(self, container_id: str):
        c = self.containers[container_id]
        c.state = ContainerState.STOPPED
        for p in c.processes:
            p.running = False
        c.logs.append("Container stopped")

    def exec(self, container_id: str, command: list[str]) -> Process:
        c = self.containers[container_id]
        if c.state != ContainerState.RUNNING:
            raise RuntimeError("Container not running")
        pid = c.next_pid; c.next_pid += 1
        proc = Process(pid, command, dict(c.env))
        c.processes.append(proc)
        c.logs.append(f"Exec PID {pid}: {' '.join(command)}")
        return proc

    def write_file(self, container_id: str, path: str, content: bytes):
        c = self.containers[container_id]
        c.rw_layer.files[path] = content

    def read_file(self, container_id: str, path: str) -> bytes:
        c = self.containers[container_id]
        fs = c.effective_fs()
        if path not in fs:
            raise FileNotFoundError(path)
        return fs[path]

    def rm(self, container_id: str):
        c = self.containers[container_id]
        if c.state == ContainerState.RUNNING:
            self.stop(container_id)
        del self.containers[container_id]

    def ps(self) -> list[dict]:
        return [{
            'id': c.id[:12], 'name': c.name, 'image': c.image.name,
            'state': c.state.name, 'pids': len([p for p in c.processes if p.running]),
            'ports': c.ports,
        } for c in self.containers.values()]


def demo():
    print("=== Container Runtime ===\n")
    rt = Runtime()

    # Build an image
    img = rt.build_image('myapp', [
        {'/bin/sh': b'#!/bin/sh\necho hello', '/etc/hostname': b'myapp'},
        {'/app/main.py': b'print("hello")', '/app/config.json': b'{}'},
    ], config={'cmd': ['python3', '/app/main.py'], 'env': {'APP_ENV': 'prod'}})
    print(f"Built image: {img.name}:{img.tag} ({img.id})")
    print(f"  Layers: {len(img.layers)}, Files: {len(img.flatten())}")

    # Create and start
    c = rt.create('myapp', name='web-1', memory_limit=256*1024*1024, ports={8080: 80})
    print(f"\nCreated: {c.name} ({c.id[:12]})")
    print(f"  Namespaces: {c.namespaces}")
    print(f"  Memory limit: {c.cgroup.memory_limit_bytes // 1024 // 1024}MB")

    rt.start(c.id)
    print(f"  State: {c.state.name}")

    # Exec
    rt.exec(c.id, ['ls', '-la'])
    rt.write_file(c.id, '/tmp/data.txt', b'runtime data')

    # Read back
    data = rt.read_file(c.id, '/tmp/data.txt')
    print(f"  Read /tmp/data.txt: {data}")

    # Container list
    print(f"\n  ps: {rt.ps()}")

    # Stop and remove
    rt.stop(c.id)
    print(f"  State: {c.state.name}")
    rt.rm(c.id)
    print(f"  Removed. Containers: {len(rt.containers)}")


if __name__ == '__main__':
    if '--test' in sys.argv:
        rt = Runtime()
        img = rt.build_image('test', [
            {'/a': b'1', '/b': b'2'},
            {'/b': b'3', '/c': b'4'},  # /b overwritten
        ])
        flat = img.flatten()
        assert flat['/a'] == b'1'
        assert flat['/b'] == b'3'  # layer 2 wins
        assert flat['/c'] == b'4'
        # Container lifecycle
        c = rt.create('test', name='t1', memory_limit=1024)
        assert c.state == ContainerState.CREATED
        rt.start(c.id)
        assert c.state == ContainerState.RUNNING
        assert len(c.processes) == 1 and c.processes[0].pid == 1
        # Exec
        p = rt.exec(c.id, ['echo', 'hi'])
        assert p.pid == 2
        # Filesystem
        rt.write_file(c.id, '/tmp/x', b'test')
        assert rt.read_file(c.id, '/tmp/x') == b'test'
        assert rt.read_file(c.id, '/a') == b'1'  # from image
        # Stop
        rt.stop(c.id)
        assert c.state == ContainerState.STOPPED
        # Cgroup
        assert c.cgroup.memory_limit_bytes == 1024
        # Namespaces
        assert len(c.namespaces) == 5
        ns_kinds = {ns.kind for ns in c.namespaces}
        assert 'pid' in ns_kinds and 'net' in ns_kinds
        # ps
        assert len(rt.ps()) == 1
        rt.rm(c.id)
        assert len(rt.ps()) == 0
        print("All tests passed ✓")
    else:
        demo()
