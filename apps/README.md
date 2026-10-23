# Source-deployment evaluation applications

These applications exercise the SDO lifecycle: source-grounded architecture capture, Kubernetes deployment, objective-specific health detectors, and controller-driven incident response. They are evaluation inputs rather than production SDO modules.

Some applications also participate in SREGym or deployment experiments. Application-specific Compose files may remain as upstream source material, but the production SDO runtime targets Kubernetes and does not generate shell health checks.

## Applications

- [deathstarbench/hotelReservation](./deathstarbench/hotelReservation)
- [deathstarbench/mediaMicroservices](./deathstarbench/mediaMicroservices)
- [deathstarbench/socialNetwork](./deathstarbench/socialNetwork)
- [fleetcast](./fleetcast)
- [onlineboutique](./onlineboutique)
- [pitstop](./pitstop)
- [sockshop](./sockshop)
- [sockshop-helidon](./sockshop-helidon)
- [teastore](./teastore)
- [teastore-saga](./teastore-saga)
- [train-ticket](./train-ticket)

Benchmark source-build declarations and usage are documented in [BUILD_DECLARATIONS.md](BUILD_DECLARATIONS.md).
