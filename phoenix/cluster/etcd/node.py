import aetcd3
import aiohttp
from functools import partial
import logging
from multimethod import multimethod
import pickle
import re
from typing import Any, Dict, Optional, Tuple, Union

from phoenix.actor import Actor, ActorId, Behaviour, Context, ExitReason
from phoenix.cluster.etcd import server
from phoenix.cluster.protocol import Send
from phoenix.dataclasses import dataclass
from phoenix.etcd import key_watcher, prefix_watcher

logger = logging.getLogger(__name__)


@dataclass
class Leading:
    """
    Leader is responsible for:

    * Removing unreachable members.
    """

    context: Context
    address: Tuple[str, int]
    etcd: aetcd3.Etcd3Client
    members: Dict[str, Tuple[str, int]]
    leader_lease: aetcd3.Lease
    member_lease: aetcd3.Lease


@dataclass
class Following:
    """"""

    context: Context
    address: Tuple[str, int]
    etcd: aetcd3.Etcd3Client
    members: Dict[str, Tuple[str, int]]
    member_lease: aetcd3.Lease


@dataclass
class RefreshLease:
    pass


class ClusterNodeExists(Exception):
    pass


"""
leader
  system1
    localhost:8080
members
  system1
    localhost:8080
  system2
    localhost:8081
"""


async def campaign(
    etcd: aetcd3.Etcd3Client, address: Tuple[str, int]
) -> Tuple[bool, Optional[str]]:
    lease = await etcd.lease(60)
    elected, _ = await etcd.transaction(
        compare=[etcd.transactions.version("/leader") == 0],
        success=[etcd.transactions.put("/leader", f"{address[0]}:{address[1]}", lease)],
        failure=[],
    )
    return elected, lease


async def start(
    context: Context, name: str, address: Tuple[str, int], etcd: Tuple[str, int]
) -> Actor:
    etcd = aetcd3.client(host=etcd[0], port=etcd[1])

    # Heartbeat for membership
    member_lease = await etcd.lease(60)

    # Join
    # Inside a transaction, check our key is not already registered
    # and if so, atomically create our key with a lease.
    joined, _ = await etcd.transaction(
        # A Create Revision of zero means the key does not exist.
        compare=[etcd.transactions.create(f"/members/{name}") == 0],
        success=[
            etcd.transactions.put(
                f"/members/{name}", f"{address[0]}:{address[1]}", member_lease
            ),
        ],
        failure=[],
    )
    if not joined:
        await etcd.close()
        raise ClusterNodeExists(name)

    server_id = await context.spawn(
        partial(server.start, host=address[0], port=address[1])
    )
    await context.link(context.actor_id, server_id)

    # Try and become the leader immediately
    elected, leader_lease = await campaign(etcd, address)

    # Watch for changes to the leader so that we can
    # attempt to become the leader if necessary.
    leader_watcher = await context.spawn(
        partial(key_watcher.start, etcd=etcd, key="/leader", notify=context.actor_id),
        name=f"{context.actor_id.value}.Etcd.KeyWatcher.Leader",
    )
    await context.cast(leader_watcher, key_watcher.Watch())
    await context.link(context.actor_id, leader_watcher)

    # Watch for changes to membership so that messages
    # can be sent to new members.
    member_watcher = await context.spawn(
        partial(
            prefix_watcher.start, etcd=etcd, prefix="/members", notify=context.actor_id
        ),
        name=f"{context.actor_id.value}.Etcd.PrefixWatcher.Members",
    )
    await context.cast(member_watcher, prefix_watcher.Watch())
    await context.link(context.actor_id, member_watcher)

    members = {}
    async for member, meta in etcd.get_prefix("/members"):
        host, port = member.split(b":")
        name = meta.key.decode("utf-8")
        m = re.match(r"/members/(\w+)", name)
        name = m.group(1)
        members[name] = (host.decode("utf-8"), int(port))

    await context.send_after(context.actor_id, RefreshLease(), delay=20)

    if elected:
        return Actor(
            state=Leading(
                context=context,
                address=address,
                etcd=etcd,
                members=members,
                leader_lease=leader_lease,
                member_lease=member_lease,
            ),
            handler=handle,
        )
    else:
        return Actor(
            state=Following(
                context=context,
                address=address,
                etcd=etcd,
                members=members,
                member_lease=member_lease,
            ),
            handler=handle,
        )


@multimethod
async def handle(state: Leading, msg: RefreshLease) -> Tuple[Behaviour, Leading]:
    await state.leader_lease.refresh()
    await state.member_lease.refresh()
    await state.context.send_after(state.context.actor_id, RefreshLease(), delay=20)
    return Behaviour.done, state


@multimethod
async def handle(state: Following, msg: RefreshLease) -> Tuple[Behaviour, Following]:
    await state.member_lease.refresh()
    await state.context.send_after(state.context.actor_id, RefreshLease(), delay=20)
    return Behaviour.done, state


@multimethod
async def handle(
    state: Union[Leading, Following], msg: aetcd3.events.PutEvent
) -> Tuple[Behaviour, Union[Leading, Following]]:
    key = msg.key.decode("utf-8")
    value = msg.value.decode("utf-8")
    if key.startswith("/members/"):
        m = re.match(r"/members/(\w+)", key)
        host, port = value.split(":")
        state.members[m.group(1)] = (host, int(port))
    return Behaviour.done, state


@multimethod
async def handle(
    state: Leading, msg: aetcd3.events.DeleteEvent
) -> Tuple[Behaviour, Leading]:
    key = msg.key.decode("utf-8")
    if key.startswith("/members/"):
        m = re.match(r"/members/(\w+)", key)
        del state.members[m.group(1)]
    elif key.startswith("/leader"):
        # We must have failed to refresh our lease.
        # Attempt to become the leader again
        elected, leader_lease = await campaign(state.etcd, state.address)
        if elected:
            state.leader_lease = leader_lease
            return Behaviour.done, state
        else:
            return Behaviour.done, Following(
                context=state.context,
                address=state.address,
                etcd=state.etcd,
                members=state.members,
                member_lease=state.member_lease,
            )
    return Behaviour.done, state


@multimethod
async def handle(
    state: Following, msg: aetcd3.events.DeleteEvent
) -> Tuple[Behaviour, Following]:
    key = msg.key.decode("utf-8")
    if key.startswith("/members/"):
        m = re.match(r"/members/(\w+)", key)
        del state.members[m.group(1)]
    elif key.startswith("/leader"):
        elected, leader_lease = await campaign(state.etcd, state.address)
        if elected:
            return Behaviour.done, Leading(
                context=state.context,
                address=state.address,
                etcd=state.etcd,
                members=state.members,
                leader_lease=leader_lease,
                member_lease=state.member_lease,
            )
        else:
            return Behaviour.done, state
    return Behaviour.done, state


@multimethod
async def handle(
    state: Union[Leading, Following], msg: Send
) -> Tuple[Behaviour, Union[Leading, Following]]:
    try:
        host, port = state.members[msg.msg.actor_id.system_id]
    except KeyError:
        logger.debug("No such ActorSystem: [%s]", msg.msg.actor_id.system_id)
        return Behaviour.done, state

    encoded = pickle.dumps(msg.msg)

    async with aiohttp.ClientSession() as session:
        async with session.post(f"http://{host}:{port}/messages", data=encoded) as resp:
            try:
                resp.raise_for_status()
            except aiohttp.ClientResponseError:
                logger.debug("", exc_info=True)
    return Behaviour.done, state


@multimethod
async def handle(state: Union[Leading, Following], msg: ExitReason):
    await state.etcd.close()
