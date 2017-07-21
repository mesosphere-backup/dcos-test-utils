#!/bin/sh
# Example ip-detect script using an external authority
# Uses the GCE metadata server to get the node's internal
# ipv4 address

curl -fsSL -H "Metadata-Flavor: Google" http://169.254.169.254/computeMetadata/v1/instance/network-interfaces/0/ip
