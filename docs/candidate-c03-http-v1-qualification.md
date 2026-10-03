# C03 HTTP qualification: physical failure retained

The new HTTP harness passed **80 targeted offline checks**, then failed its first
real-container mechanics control. **One control failed; eight did not run.** No
HTTP acceptance, project-quality result or swarm comparison is claimed.

The implementation adds a source-bound server, a fixed isolated wire probe,
explicit normal restart epochs, and durable execution journals. It retains raw
responses and separates a complete HTTP message from socket closure, server
continuity and product correctness. The nine-control roster was frozen before
physical execution; its fixtures and expected outcomes remain unchanged.

## What the physical run found

The first helper started, returned a zero exit status and produced a complete
captured transcript. Docker changed its configured hostname to the current
server's hostname when it joined that server's network namespace. The v1 checker
did not permit this transition, so it withheld authentication and stopped the
history. The other eight controls never started.

The retained transcript contains the first declared request and a framed
response, but remains diagnostic data. There are **zero authenticated request
outcomes**, one unknown attributed outcome, and no transcript for the other
thirteen planned rows. The three owned containers and their volume were removed
with retained absence evidence. The failed run was not retried or relabeled.

The exact pinned [Docker implementation](https://github.com/moby/moby/blob/6bc6209b88a7a834c91f77d848e025c79e0227a1/daemon/container_operations.go#L411-L424)
copies the network donor's hostname and domain during startup. This is a missing
runtime compatibility rule in the harness, not evidence about an agent's software
quality. It also illustrates why the offline checks did not replace physical
qualification: their inert lifecycle did not model this real transition.

## Correction and verification

The [prospective v2 plan](../analysis/candidate-c03-http-probe-hostname-plan-v2.json)
requires donor metadata from the independently verified current server before
helper creation. Only the exact probe startup hostname transition will be
admitted under the pinned runtime. Other fields, role restrictions, network
target, source checks and server continuity remain exact. The current profile
keeps explicit empty domain values; arbitrary domain changes are not admitted.

V1 source and failure evidence remain frozen. V2 needs source review, negative
and composed offline regressions, then a newly frozen complete physical run.
Neither this plan nor the existing wire transcript qualifies that future run.

The [checkpoint](../analysis/candidate-c03-http-v1-qualification-checkpoint.json)
binds the exact sources, offline execution, definition freeze, physical failure,
independent audit and remaining work. The [audit](../analysis/candidate-c03-http-v1-evidence-audit.json)
separates observed facts from authenticated outcomes and prospective counts.

## Research meaning

This cycle made no provider calls and added no comparative project samples.
The full HTTP product catalog, semantic observer, cross-interface evidence,
production acceptance bridge and larger cumulative study remain unfinished.
Quality and sustained completion remain the study's primary outcomes. The prior
live evidence still shows no demonstrated general swarm advantage.
