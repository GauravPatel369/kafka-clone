"""
CONCEPT: Assignment Strategies — Dividing Partitions Among Members
---------------------------------------------------------------------
When a group rebalances, somebody has to decide which member reads which
partition. That somebody is the group LEADER — an ordinary client — not the
coordinator.

That is a deliberate separation, and it is why this file lives in `consumer/`
rather than `broker/`:

    the broker's job    membership, generations, liveness, fencing
    the client's job    policy — who gets what

So a custom strategy (rack-aware, weighted by consumer capacity, sticky across
rebalances) ships with your application and never touches a broker. Kafka has
half a dozen assignors and one coordinator, and this is why.


THE INVARIANT EVERY ASSIGNOR MUST HOLD
----------------------------------------
**Each partition goes to exactly one member.** Assign one twice and two
consumers process the same messages; assign it zero times and it silently stops
being read. Neither raises an error anywhere — which is why the tests here
check the invariant directly rather than checking any particular layout.


RANGE vs ROUND-ROBIN
----------------------
Both are implemented. They differ in what they optimise:

    RANGE — per topic, hand out contiguous runs.

        orders  p0 p1 p2 p3      A: orders-0, orders-1
        3 members                B: orders-2
                                 C: orders-3

      + Co-partitioning: with two topics partitioned the same way, member A
        gets orders-0 AND payments-0. If you are joining those streams, the
        data you need is on the same consumer. That is a real and specific
        benefit, and it is why range is the default.
      - Uneven when partitions do not divide evenly — and always uneven in the
        SAME direction, so the early members are permanently busier.

    ROUND-ROBIN — deal every (topic, partition) out in turn.

        orders p0 p1 p2, payments p0 p1     A: orders-0, payments-0
        3 members                           B: orders-1, payments-1
                                            C: orders-2

      + Best balance across many topics; the skew never exceeds one partition.
      - No co-partitioning: related partitions of different topics land on
        different consumers.

Neither is "better". Range trades balance for locality; round-robin does the
reverse. Kafka defaults to range and lets you say otherwise.

**Not implemented: sticky.** It balances *and* minimises how many partitions
move between rebalances, which matters because a moved partition means the new
owner rebuilds whatever local state it had. Kafka added it years after the
other two, and it is substantially more complex.
"""


def _all_partitions(subscriptions: dict, partition_counts: dict) -> list:
    """
    Every (topic, partition) any member subscribed to, in a stable order.

    Sorted, and that is load-bearing: every member computing the same
    assignment from the same inputs must get the same answer. Iterating a set
    or a dict without sorting would let two members disagree — and in this
    design only the leader computes, so the real risk is a rebalance producing
    a *different* layout every round for no reason, moving partitions
    pointlessly.
    """
    topics = sorted({topic for topics in subscriptions.values() for topic in topics})
    return [(topic, partition)
            for topic in topics
            for partition in range(partition_counts.get(topic, 0))]


def range_assign(subscriptions: dict, partition_counts: dict) -> dict:
    """
    Per topic, give each member a contiguous run of partitions.

    `subscriptions`     member_id -> [topic, ...]
    `partition_counts`  topic -> number of partitions
    Returns             member_id -> [(topic, partition), ...]

    The remainder goes to the earliest members, which is why range is unevenly
    balanced *in a fixed direction*: with 7 partitions and 3 members the split
    is 3/2/2, and the same member is over-loaded after every rebalance.
    """ 
    assignment = {member_id: [] for member_id in subscriptions}

    topics = sorted({topic for topics in subscriptions.values() for topic in topics})

    for topic in topics:
        # Only members that actually subscribed to this topic. A group can
        # legitimately hold members with different subscriptions.
        members = sorted(m for m, topics_ in subscriptions.items() if topic in topics_)
        if not members:
            continue

        count = partition_counts.get(topic, 0)
        per_member, remainder = divmod(count, len(members))

        start = 0
        for i, member_id in enumerate(members):
            # The first `remainder` members get one extra.
            take = per_member + (1 if i < remainder else 0)
            assignment[member_id].extend(
                (topic, p) for p in range(start, start + take))
            start += take

    return assignment


def round_robin_assign(subscriptions: dict, partition_counts: dict) -> dict:
    """
    Deal every partition out in turn, across all topics.

    Balance is optimal — no member differs from another by more than one
    partition — at the cost of the co-partitioning that range provides.

    Members with different subscriptions are handled by skipping any member
    that did not subscribe to the topic being dealt. Without that skip, a
    member would be assigned a partition of a topic it is not reading, and
    those messages would silently stop being consumed.
    """
    assignment = {member_id: [] for member_id in subscriptions}
    members = sorted(subscriptions)
    if not members:
        return assignment

    index = 0
    for topic, partition in _all_partitions(subscriptions, partition_counts):
        # Advance until we find a member subscribed to this topic. Bounded by
        # one full lap so an unsubscribed topic cannot spin forever.
        for _ in range(len(members)):
            member_id = members[index % len(members)]
            index += 1
            if topic in subscriptions[member_id]:
                assignment[member_id].append((topic, partition))
                break

    return assignment


ASSIGNORS = {
    'range': range_assign,
    'roundrobin': round_robin_assign,
}


def assign(protocol: str, subscriptions: dict, partition_counts: dict) -> dict:
    """
    Run the named strategy. Unknown names fall back to range, matching Kafka's
    default rather than failing a rebalance over a config typo — a group that
    cannot form is worse than one balanced slightly differently than intended.
    """
    return ASSIGNORS.get(protocol, range_assign)(subscriptions, partition_counts)
