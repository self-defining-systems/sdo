// One build graph shares the Go/runtime stages and passes the responder directly
// to the benchmark extension, including with a docker-container builder.
variable "SDO_IMAGE_TAG" { default = "v0.1.0" }
variable "SDO_VALIDATOR_SCHEMA_IDENTITY" { default = "unknown" }

group "default" {
  targets = ["controller", "validator", "responder", "benchmark"]
}

target "controller" {
  context = "."
  dockerfile = "sdo/operator/Dockerfile.runtime"
  target = "controller"
  tags = ["sdo-controller:${SDO_IMAGE_TAG}"]
}

target "validator" {
  context = "."
  dockerfile = "sdo/operator/Dockerfile.validator"
  args = { SDO_VALIDATOR_SCHEMA_IDENTITY = SDO_VALIDATOR_SCHEMA_IDENTITY }
  tags = ["sdo-detector-validator:${SDO_IMAGE_TAG}"]
}

target "responder" {
  context = "."
  dockerfile = "sdo/operator/Dockerfile.runtime"
  target = "responder"
  tags = ["sdo-responder:${SDO_IMAGE_TAG}"]
}

target "benchmark" {
  context = "."
  dockerfile = "benchmarks/sregym/Dockerfile.worker"
  target = "sregym-responder"
  args = { SDO_RESPONDER_IMAGE = "responder-base" }
  contexts = { responder-base = "target:responder" }
  tags = ["sdo-sregym-responder:${SDO_IMAGE_TAG}"]
}
