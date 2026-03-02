variable "VERSION" {
  default = ""
}

variable "REVISION" {
  default = ""
}

variable "BUILD_DATE" {
  default = ""
}

group "default" {
  targets = ["query", "ingest", "maintain"]
}

target "_common" {
  context    = "."
  dockerfile = "config/docker/Dockerfile.release"
  platforms  = ["linux/amd64", "linux/arm64"]
  // Each binary target builds the full Dockerfile independently.
  // BuildKit deduplicates the builder stage layers across targets
  // within the same bake invocation. A separate "builder" bake target
  // with target:builder context sharing is NOT used because it breaks
  // per-platform tracking when the builder is pinned to $BUILDPLATFORM.
  target = "runtime"
}

target "query" {
  inherits = ["_common"]
  args = {
    BINARY      = "query"
    DESCRIPTION = "Query APIs for IceGate (Loki, Prometheus, Tempo)"
    VERSION     = VERSION
    REVISION    = REVISION
    BUILD_DATE  = BUILD_DATE
  }
}

target "ingest" {
  inherits = ["_common"]
  args = {
    BINARY      = "ingest"
    DESCRIPTION = "OTLP data ingestion for IceGate"
    VERSION     = VERSION
    REVISION    = REVISION
    BUILD_DATE  = BUILD_DATE
  }
}

target "maintain" {
  inherits = ["_common"]
  args = {
    BINARY      = "maintain"
    DESCRIPTION = "Maintenance operations for IceGate"
    VERSION     = VERSION
    REVISION    = REVISION
    BUILD_DATE  = BUILD_DATE
  }
}
