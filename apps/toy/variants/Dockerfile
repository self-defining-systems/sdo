# Buildable from the source tree alone (no prebuilt binary), so a deployment
# source can build the image from Git. The base is the digest-pinned Go image
# the repository's own builds use.
FROM golang:1.26.8-bookworm@sha256:a688600ca24f8a4d3ca77f95b0dd40704a9fc787c826660eb7ba0b641b8b175d AS build
WORKDIR /src
COPY main.go .
RUN CGO_ENABLED=0 go build -trimpath -o /out/toy main.go

FROM scratch
COPY --from=build /out/toy /toy
USER 65532:65532
ENTRYPOINT ["/toy"]
