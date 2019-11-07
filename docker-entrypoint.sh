#!/bin/sh

echo "args: $@"

exec /usr/local/bin/debeclient "$@ &"