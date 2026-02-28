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
}

target "query" {
  inherits = ["_common"]
  args = {
    BINARY     = "query"
    VERSION    = VERSION
    REVISION   = REVISION
    BUILD_DATE = BUILD_DATE
  }
}

target "ingest" {
  inherits = ["_common"]
  args = {
    BINARY     = "ingest"
    VERSION    = VERSION
    REVISION   = REVISION
    BUILD_DATE = BUILD_DATE
  }
}

target "maintain" {
  inherits = ["_common"]
  args = {
    BINARY     = "maintain"
    VERSION    = VERSION
    REVISION   = REVISION
    BUILD_DATE = BUILD_DATE
  }
}
