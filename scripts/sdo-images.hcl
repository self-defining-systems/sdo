// One build graph shares the Go/runtime stages and passes the responder directly
// to the benchmark extension, including with a docker-container builder.
variable "SDO_SOURCE_REVISION" { default = "unknown" }
variable "SDO_CONTROLLER_SOURCE_TREE" { default = "unknown" }
variable "SDO_CONTROLLER_MANIFEST_TREE" { default = "unknown" }
variable "SDO_RESPONDER_SOURCE_TREE" { default = "unknown" }
variable "SDO_RESPONDER_MANIFEST_TREE" { default = "unknown" }
variable "SDO_BENCHMARK_SOURCE_TREE" { default = "unknown" }
variable "SDO_BENCHMARK_MANIFEST_TREE" { default = "unknown" }
variable "SDO_IMAGE_TAG" { default = "v0.1.0" }
variable "SDO_WORKER_TARGET" { default = "sregym-responder" }

group "default" {
  targets = ["controller", "responder", "benchmark", "script-tools"]
}

target "controller" {
  labels = {
    "org.opencontainers.image.revision" = SDO_SOURCE_REVISION
    "dev.sdo.source-tree" = SDO_CONTROLLER_SOURCE_TREE
    "dev.sdo.manifest-tree" = SDO_CONTROLLER_MANIFEST_TREE
    "dev.sdo.image-target" = "controller"
  }
  context = "."
  dockerfile = "sdo/operator/Dockerfile.runtime"
  target = "controller"
  tags = ["sdo-controller:${SDO_IMAGE_TAG}"]
}


target "responder" {
  labels = {
    "org.opencontainers.image.revision" = SDO_SOURCE_REVISION
    "dev.sdo.source-tree" = SDO_RESPONDER_SOURCE_TREE
    "dev.sdo.manifest-tree" = SDO_RESPONDER_MANIFEST_TREE
    "dev.sdo.image-target" = "responder"
  }
  context = "."
  dockerfile = "sdo/operator/Dockerfile.runtime"
  target = "responder"
  tags = ["sdo-responder:${SDO_IMAGE_TAG}"]
}

target "benchmark" {
  labels = {
    "org.opencontainers.image.revision" = SDO_SOURCE_REVISION
    "dev.sdo.source-tree" = SDO_BENCHMARK_SOURCE_TREE
    "dev.sdo.manifest-tree" = SDO_BENCHMARK_MANIFEST_TREE
    "dev.sdo.image-target" = "benchmark"
    "dev.sdo.worker-target" = SDO_WORKER_TARGET
  }
  context = "."
  dockerfile = "benchmarks/sregym/Dockerfile.worker"
  target = SDO_WORKER_TARGET
  args = { SDO_RESPONDER_IMAGE = "responder-base", SDO_SCRIPTED_BASE_IMAGE = "scripted-base" }
  contexts = { responder-base = "target:responder", scripted-base = "target:scripted-runtime" }
  tags = ["sdo-sregym-responder:${SDO_IMAGE_TAG}"]
}

target "scripted-runtime" {
 context = "."
 dockerfile = "sdo/operator/Dockerfile.runtime"
 target = "scripted-runtime"
}

target "script-tools" {
  context = "."
  dockerfile = "sdo/operator/Dockerfile.runtime"
  target = "script-tools"
  tags = ["sdo-script-tools:${SDO_IMAGE_TAG}"]
}
