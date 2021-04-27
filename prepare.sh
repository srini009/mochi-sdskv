#!/bin/bash

git config core.hooksPath maint/hooks 2> /dev/null || true

autoreconf -fi
