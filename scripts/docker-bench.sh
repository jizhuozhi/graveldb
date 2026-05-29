#!/bin/sh
# GravelDB - Docker benchmark script
# Wrapper around docker-test.sh with bench mode enabled.
exec sh "$(dirname "$0")/docker-test.sh" bench
