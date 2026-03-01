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
  platforms  = ["linux/amd64"]  # TODO(medium): Investigate a slow ARM compilation
}

// Builds chef → planner → builder once. Not in the default group,
// but bake resolves it automatically via the "target:builder" context
// reference in _runtime.
target "builder" {
  inherits = ["_common"]
  target   = "builder"
}

// Runtime targets override the "builder" named stage with the single
// builder target output, so COPY --from=builder resolves to it without
// rebuilding chef/planner/builder per binary.
target "_runtime" {
  inherits = ["_common"]
  contexts = {
    builder = "target:builder"
  }
  target = "runtime"
}

target "query" {
  inherits = ["_runtime"]
  args = {
    BINARY     = "query"
    VERSION    = VERSION
    REVISION   = REVISION
    BUILD_DATE = BUILD_DATE
  }
}

target "ingest" {
  inherits = ["_runtime"]
  args = {
    BINARY     = "ingest"
    VERSION    = VERSION
    REVISION   = REVISION
    BUILD_DATE = BUILD_DATE
  }
}

target "maintain" {
  inherits = ["_runtime"]
  args = {
    BINARY     = "maintain"
    VERSION    = VERSION
    REVISION   = REVISION
    BUILD_DATE = BUILD_DATE
  }
}
