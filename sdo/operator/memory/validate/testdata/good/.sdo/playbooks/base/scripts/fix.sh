#!/bin/bash
set -eu
kubectl rollout restart "$1"
